import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import * as Y from "yjs";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { z } from "zod";
import { GraphQLClient } from "../graphqlClient.js";
import { text } from "../util/mcp.js";
import {
  connectWorkspaceSocket,
  joinWorkspace,
  loadDoc,
  wsUrlFromGraphQLEndpoint,
} from "../ws.js";

/**
 * AFFiNE workspace export as a `.affine` backup file.
 *
 * The `.affine` format (nbstore v1, see AFFiNE
 * packages/frontend/native/schema/src/v1.rs) is a SQLite database with five
 * tables: `updates` (Yjs doc state per doc_id), `blobs` (attachment binaries
 * keyed by blob key), `version_info`, `server_clock`, and `sync_metadata`.
 * AFFiNE's desktop app produces it via `pool.vacuumInto()` (a SQLite
 * VACUUM INTO of the local workspace database) and restores it with
 * `loadDBFile()` + `validateImportSchema()` (which only checks the table
 * structure, not content).
 *
 * This export re-creates an equivalent database server-side:
 *   1. enumerate doc ids via GraphQL (plus the workspace root doc),
 *   2. download each doc's merged Yjs state from
 *      GET /api/workspaces/:id/docs/:guid,
 *   3. scan each doc's blocks for blob references (prop:sourceId),
 *   4. download each referenced blob from GET /api/workspaces/:id/blobs/:name,
 *   5. write everything into a nbstore-v1-shaped SQLite file via node:sqlite.
 */

// nbstore v1 schema — column names must match AFFiNE's V1_IMPORT_SCHEMA_RULES
// exactly or `loadDBFile` will reject the file with DB_FILE_INVALID.
const NBSTORE_V1_SCHEMA = `
CREATE TABLE IF NOT EXISTS "updates" (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  data BLOB NOT NULL,
  timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
  doc_id TEXT
);
CREATE TABLE IF NOT EXISTS "blobs" (
  key TEXT PRIMARY KEY NOT NULL,
  data BLOB NOT NULL,
  timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL
);
CREATE TABLE IF NOT EXISTS "version_info" (
  version NUMBER NOT NULL,
  timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL
);
CREATE TABLE IF NOT EXISTS "server_clock" (
  key TEXT PRIMARY KEY NOT NULL,
  data BLOB NOT NULL,
  timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL
);
CREATE TABLE IF NOT EXISTS "sync_metadata" (
  key TEXT PRIMARY KEY NOT NULL,
  data BLOB NOT NULL,
  timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL
);
`;

const DOC_LIST_QUERY = `query WorkspaceDocs($workspaceId: String!) {
  workspace(id: $workspaceId) {
    docs(pagination: { first: 200 }) {
      edges {
        node {
          id
        }
      }
    }
  }
}`;

const DOWNLOAD_CONCURRENCY = 4;
const MAX_RESPONSE_BYTES = 512 * 1024 * 1024; // 512 MiB safety cap per response

type DocListResponse = {
  workspace: {
    docs: { edges: Array<{ node: { id: string } }> } | null;
  } | null;
};

/** Run `fn` over `items` with at most `limit` promises in flight. */
async function mapLimit<T, R>(
  items: T[],
  limit: number,
  fn: (item: T, index: number) => Promise<R>
): Promise<R[]> {
  const results = new Array<R>(items.length);
  let nextIndex = 0;
  const workers = Array.from(
    { length: Math.min(limit, items.length) },
    async () => {
      while (nextIndex < items.length) {
        const index = nextIndex;
        nextIndex += 1;
        results[index] = await fn(items[index], index);
      }
    }
  );
  await Promise.all(workers);
  return results;
}

/** Collect blob references (prop:sourceId) from every block in a Yjs doc. */
function collectBlobKeys(bin: Uint8Array): string[] {
  const doc = new Y.Doc();
  Y.applyUpdate(doc, bin);
  const keys = new Set<string>();
  const blocks = doc.getMap("blocks");
  blocks.forEach((value: unknown) => {
    if (!(value instanceof Y.Map)) {
      return;
    }
    const sourceId = value.get("prop:sourceId");
    if (typeof sourceId === "string" && sourceId.trim().length > 0) {
      keys.add(sourceId.trim());
    }
  });
  return [...keys];
}

/**
 * Fallback doc enumeration: join the workspace over WebSocket, load the root
 * doc, and read page ids from its meta.pages.
 */
async function readWorkspacePageIdsFromRootDoc(
  gql: GraphQLClient,
  workspaceId: string
): Promise<string[]> {
  const { endpoint, cookie, bearer } = await gql.getConnectionAuth();
  const socket = await connectWorkspaceSocket(wsUrlFromGraphQLEndpoint(endpoint), cookie, bearer);
  try {
    await joinWorkspace(socket, workspaceId);
    const snapshot = await loadDoc(socket, workspaceId, workspaceId);
    const bin = snapshot.missing ?? snapshot.state;
    if (!bin) {
      return [];
    }
    const doc = new Y.Doc();
    Y.applyUpdate(doc, Buffer.from(bin, "base64"));
    const meta = doc.getMap("meta");
    const pages = meta.get("pages");
    if (!(pages instanceof Y.Array)) {
      return [];
    }
    const ids: string[] = [];
    pages.forEach((value: unknown) => {
      if (value instanceof Y.Map) {
        const id = value.get("id");
        if (typeof id === "string" && id.length > 0) {
          ids.push(id);
        }
      }
    });
    return ids;
  } finally {
    socket.disconnect();
  }
}

async function downloadBinary(url: string, headers: Record<string, string>): Promise<Uint8Array> {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), 120_000);
  try {
    const res = await fetch(url, { headers, signal: controller.signal });
    if (!res.ok) {
      throw new Error(`HTTP ${res.status} ${res.statusText}`);
    }
    const contentLength = Number(res.headers.get("content-length") ?? 0);
    if (contentLength > MAX_RESPONSE_BYTES) {
      throw new Error(`response too large (${contentLength} bytes > ${MAX_RESPONSE_BYTES})`);
    }
    const buffer = await res.arrayBuffer();
    if (buffer.byteLength > MAX_RESPONSE_BYTES) {
      throw new Error(`response too large (${buffer.byteLength} bytes > ${MAX_RESPONSE_BYTES})`);
    }
    return new Uint8Array(buffer);
  } finally {
    clearTimeout(timer);
  }
}

export function registerWorkspaceExportTools(
  server: McpServer,
  gql: GraphQLClient,
  defaults: { workspaceId?: string; exportDir?: string }
): void {
  const exportWorkspaceHandler = async (params: {
    workspaceId?: string;
    outputPath?: string;
    includeBlobs?: boolean;
  }): Promise<ReturnType<typeof text>> => {
    const workspaceId = params.workspaceId || defaults.workspaceId;
    if (!workspaceId) {
      throw new Error(
        "workspaceId is required. Provide it as a parameter or set AFFINE_WORKSPACE_ID in environment."
      );
    }
    const includeBlobs = params.includeBlobs !== false;

    const { endpoint, headers } = await gql.getConnectionAuth();
    const baseUrl = new URL(endpoint).origin;
    const wsPart = encodeURIComponent(workspaceId);

    // 1. Enumerate documents (GraphQL doc list + the workspace root doc).
    const docIds = new Set<string>();
    try {
      const data = await gql.request<DocListResponse>(DOC_LIST_QUERY, { workspaceId });
      for (const edge of data.workspace?.docs?.edges ?? []) {
        const docId = edge?.node?.id;
        if (typeof docId === "string" && docId.length > 0) {
          docIds.add(docId);
        }
      }
    } catch (err) {
      // Workspace.docs requires Workspace.Users.Manage. If the caller lacks it,
      // fall back to the workspace root doc's meta.pages (WS snapshot), which
      // only needs workspace join access.
      try {
        const wsDocIds = await readWorkspacePageIdsFromRootDoc(gql, workspaceId);
        for (const docId of wsDocIds) {
          docIds.add(docId);
        }
      } catch (fallbackErr) {
        throw new Error(
          `Failed to list workspace documents: ${(err as Error).message} ` +
            `(meta.pages fallback also failed: ${(fallbackErr as Error).message})`
        );
      }
    }
    docIds.add(workspaceId); // workspace root doc (meta: pages, tags, ...)

    // 2. Download each document's merged Yjs state.
    const docBins = new Map<string, Uint8Array>();
    const docFailures: Array<{ docId: string; error: string }> = [];
    await mapLimit([...docIds], DOWNLOAD_CONCURRENCY, async (docId) => {
      try {
        const bin = await downloadBinary(
          `${baseUrl}/api/workspaces/${wsPart}/docs/${encodeURIComponent(docId)}`,
          headers
        );
        docBins.set(docId, bin);
      } catch (err) {
        docFailures.push({ docId, error: (err as Error).message });
      }
    });

    if (docBins.size === 0) {
      throw new Error(
        `No documents could be downloaded for workspace '${workspaceId}' (${docFailures.length} failed). ` +
          `Check that the workspace exists and the authenticated user can read it.`
      );
    }

    // 3. Collect blob references across all docs.
    const blobKeys = new Set<string>();
    for (const bin of docBins.values()) {
      try {
        for (const key of collectBlobKeys(bin)) {
          blobKeys.add(key);
        }
      } catch {
        // A doc that fails to parse as Yjs still contributes no blob refs.
      }
    }

    // 4. Download blobs (best-effort; missing blobs are reported, not fatal).
    const blobData = new Map<string, Uint8Array>();
    const blobFailures: Array<{ key: string; error: string }> = [];
    if (includeBlobs && blobKeys.size > 0) {
      await mapLimit([...blobKeys], DOWNLOAD_CONCURRENCY, async (key) => {
        try {
          const data = await downloadBinary(
            `${baseUrl}/api/workspaces/${wsPart}/blobs/${encodeURIComponent(key)}`,
            headers
          );
          blobData.set(key, data);
        } catch (err) {
          blobFailures.push({ key, error: (err as Error).message });
        }
      });
    }

    // 5. Write the nbstore-v1 SQLite database.
    // Priority: explicit outputPath > AFFINE_EXPORT_DIR > system temp dir.
    const outputPath =
      params.outputPath?.trim() ||
      (defaults.exportDir
        ? path.join(defaults.exportDir, `affine-mcp-export-${workspaceId}-${Date.now()}.affine`)
        : path.join(os.tmpdir(), `affine-mcp-export-${workspaceId}-${Date.now()}.affine`));
    fs.mkdirSync(path.dirname(outputPath), { recursive: true });

    let DatabaseSync: new (location: string) => {
      exec(sql: string): void;
      prepare(sql: string): {
        run(...params: Array<Uint8Array | string | number | null>): unknown;
      };
      close(): void;
    };
    try {
      ({ DatabaseSync } = await import("node:sqlite"));
    } catch {
      throw new Error(
        "Exporting .affine requires node:sqlite (Node.js >= 22.13). " +
          `Current runtime is ${process.version}. Upgrade Node or use AFFiNE desktop's built-in Full Backup instead.`
      );
    }

    const db = new DatabaseSync(outputPath);
    try {
      db.exec(NBSTORE_V1_SCHEMA);
      db.prepare("INSERT INTO version_info (version) VALUES (?)").run(3);

      const insertUpdate = db.prepare("INSERT INTO updates (data, doc_id) VALUES (?, ?)");
      for (const [docId, bin] of docBins) {
        // nbstore v1 convention (WorkspaceSQLiteDB.toDBDocId): the workspace
        // root doc is stored with doc_id = NULL; only regular docs carry their
        // docId. Writing the old workspace id here makes the imported workspace
        // unreadable because the new workspace looks up its root doc via
        // `WHERE doc_id IS NULL`.
        const dbDocId = docId === workspaceId ? null : docId;
        insertUpdate.run(bin, dbDocId);
      }

      const insertBlob = db.prepare("INSERT INTO blobs (key, data) VALUES (?, ?)");
      for (const [key, data] of blobData) {
        insertBlob.run(key, data);
      }
    } finally {
      db.close();
    }

    const totalDocBytes = [...docBins.values()].reduce((sum, bin) => sum + bin.byteLength, 0);
    const totalBlobBytes = [...blobData.values()].reduce((sum, data) => sum + data.byteLength, 0);

    return text({
      success: true,
      workspaceId,
      filePath: outputPath,
      format: "affine",
      docCount: docBins.size,
      blobCount: blobData.size,
      blobSkipped: !includeBlobs && blobKeys.size > 0 ? blobKeys.size : 0,
      docDownloadFailures: docFailures,
      blobDownloadFailures: blobFailures,
      docBytes: totalDocBytes,
      blobBytes: totalBlobBytes,
      totalBytes: totalDocBytes + totalBlobBytes,
      notes:
        "Importable from AFFiNE desktop via workspace settings > Storage > Full Backup import (generates a new local workspace).",
    });
  };

  server.registerTool(
    "export_workspace",
    {
      title: "Export Workspace",
      description:
        "Export an entire AFFiNE workspace as a .affine backup file (nbstore v1 SQLite: all documents' Yjs state in 'updates' + attachments in 'blobs'). Downloads every document via the workspace REST API, scans for blob references, downloads attachments, and writes a database that AFFiNE desktop can import as a new local workspace. Does not modify any workspace data.",
      inputSchema: {
        workspaceId: z.string().optional().describe("Workspace ID (optional if default set)"),
        outputPath: z
          .string()
          .optional()
          .describe("Absolute output file path, ideally ending in .affine. Defaults to a generated file in the system temp directory."),
        includeBlobs: z
          .boolean()
          .optional()
          .describe("Download and embed attachments (blobs). Default true. Set false for a doc-only export."),
      },
    },
    exportWorkspaceHandler as never
  );
}
