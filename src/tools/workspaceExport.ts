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
  loadDocTimestamps,
  wsUrlFromGraphQLEndpoint,
} from "../ws.js";

/**
 * AFFiNE workspace export as a `.affine` backup file.
 *
 * The `.affine` format is a SQLite database. This export writes the nbstore v2
 * layout (meta / snapshots / updates / clocks / blobs / peer_clocks — see AFFiNE
 * packages/frontend/native/schema/src/import_validation.rs, V2_IMPORT_SCHEMA_RULES)
 * so AFFiNE desktop's `loadDBFile` takes the v2 import path (`setSpaceId` etc.).
 * v1 imports route through `cpV1DBFile`, which never surfaces workspace sub-docs
 * (icons/folders) to the WorkspaceDB — that is why the export must be v2.
 *
 * This export re-creates an equivalent database server-side:
 *   1. enumerate doc ids via WS `space:load-doc-timestamps` (which includes
 *      workspace sub-docs like db$<wsId>$folders / $docProperties / $explorerIcon
 *      that GraphQL's workspace.docs omits), plus known sub-doc patterns that
 *      the timestamps endpoint may not have synced yet,
 *   2. download each doc's merged Yjs state from
 *      GET /api/workspaces/:id/docs/:guid,
 *   3. scan each doc's blocks for blob references (prop:sourceId),
 *   4. download each referenced blob from GET /api/workspaces/:id/blobs/:name,
 *   5. write everything into a nbstore-v2-shaped SQLite file via node:sqlite.
 */

// Workspace sub-docs hold sidebar/page icons, doc properties, folders, and
// collections (see AFFiNE packages/frontend/core/src/modules/db/schema). They
// are NOT listed by GraphQL workspace.docs. Both legacy (db$<table>) and current
// (db$<workspaceId>$<table>) guid forms are attempted; missing ones are skipped.
const WORKSPACE_SUBDOC_TABLES = [
  "explorerIcon",
  "docProperties",
  "docCustomPropertyInfo",
  "folders",
  "pinnedCollections",
] as const;

function workspaceSubDocCandidates(workspaceId: string): string[] {
  const legacy = WORKSPACE_SUBDOC_TABLES.map((table) => `db$${table}`);
  const current = WORKSPACE_SUBDOC_TABLES.map((table) => `db$${workspaceId}$${table}`);
  return [...legacy, ...current];
}

/**
 * AFFiNE's desktop WorkspaceDB addresses tables as `db$<table>` (see
 * packages/frontend/core/src/modules/db/services/db.ts, storageDocId:
 * `db$${tableName}`), but the server persists them as `db$<workspaceId>$<table>`.
 * On import the desktop app generates a NEW workspace id, so a sub-doc stored
 * under the old `db$<workspaceId>$<table>` guid would never be found again. Map
 * the server-side guid to the desktop-visible `db$<table>` form so icons,
 * folders and doc properties survive the import. Unknown/plain doc ids pass
 * through unchanged.
 *
 * Userdata docs (favorites/settings) follow the same scheme with a different
 * prefix (packages/common/nbstore/src/utils/id-converter.ts): the server stores
 * `userdata$<userId>$<workspaceId>$<table>` while the desktop's local workspace
 * reads `userdata$__local__$<table>`. Convert those too so favorites survive
 * the import into the new local workspace.
 */
function toDesktopSubDocId(docId: string, workspaceId: string): string {
  const dbPrefix = `db$${workspaceId}$`;
  if (docId.startsWith(dbPrefix)) {
    return `db$${docId.slice(dbPrefix.length)}`;
  }
  // userdata$<userId>$<workspaceId>$<table> -> userdata$__local__$<table>
  const userdataMatch = docId.match(new RegExp(`^userdata\\$[\\w-]+\\$${workspaceId}\\$(.+)$`));
  if (userdataMatch) {
    return `userdata$__local__$${userdataMatch[1]}`;
  }
  return docId;
}

// sqlx migration bookkeeping for nbstore v2 (see AFFiNE
// packages/frontend/native/schema/src/lib.rs, MIGRATIONS). The desktop app's
// DocStorage.set_space_id() -> connect() -> migrate() replays these migrations
// against the imported file. Without a fully-applied _sqlx_migrations table the
// migrator re-runs migration 1 (init_v2), whose `CREATE TABLE "meta"` (no
// IF NOT EXISTS) fails because the tables already exist — the import then
// errors out with UNKNOWN_ERROR. A real desktop export carries these rows, so
// we must too; the checksums are SHA-384 of the exact migration SQL and match
// what a desktop-generated workspace db stores.
const SQLX_MIGRATIONS: ReadonlyArray<{ version: number; description: string; checksumHex: string }> = [
  {
    version: 1,
    description: "init_v2",
    checksumHex:
      "a1f0a1496ba1d1ff1689fc234514b13e7501ce5a3891b5943a75300b20e68444444ae71a1a80f40e46ccee2fc9e2af1a",
  },
  {
    version: 2,
    description: "add_blob_sync",
    checksumHex:
      "c40244fec04822d74db419bead8486db435bb52d1f0214e4ddcc8928f4444475c7be8f96cee4c4383fd21b866fffd630",
  },
  {
    version: 3,
    description: "add_idx_snapshots",
    checksumHex:
      "c13e51745e6f2d3e49f01fc82df68e88e91feabfa0a28507d658bb85b4c5cb81354ac8b23eb1038b175b1ae66311980a",
  },
  {
    version: 4,
    description: "add_indexer_sync",
    checksumHex:
      "eeb9b2d07c3827f326feaed6651f587f177c2312c701d72f321c2d1a132bcd962fc914b39b12a34694003033bf29882b",
  },
];

// nbstore v2 schema — matches AFFiNE's V2_IMPORT_SCHEMA_RULES (see
// packages/frontend/native/schema/src/import_validation.rs). The desktop app's
// `loadDBFile` validates against THIS schema first (meta/snapshots/updates/
// clocks/blobs/peer_clocks required). Exporting v2 makes the import take the
// v2 path (setSpaceId etc.), where workspace sub-docs such as db$explorerIcon
// are loaded by the WorkspaceDB — v1 imports never surface those sub-docs.
const NBSTORE_V2_SCHEMA = `
CREATE TABLE IF NOT EXISTS "_sqlx_migrations" (
  version BIGINT PRIMARY KEY,
  description TEXT NOT NULL,
  installed_on TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  success BOOLEAN NOT NULL,
  checksum BLOB NOT NULL,
  execution_time BIGINT NOT NULL
);
CREATE TABLE IF NOT EXISTS "meta" (
  space_id VARCHAR NOT NULL PRIMARY KEY
);
CREATE TABLE IF NOT EXISTS "snapshots" (
  doc_id VARCHAR NOT NULL PRIMARY KEY,
  data BLOB NOT NULL,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP NOT NULL
);
CREATE TABLE IF NOT EXISTS "updates" (
  doc_id VARCHAR NOT NULL,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  data BLOB NOT NULL,
  PRIMARY KEY (doc_id, created_at)
);
CREATE TABLE IF NOT EXISTS "clocks" (
  doc_id VARCHAR NOT NULL PRIMARY KEY,
  timestamp TIMESTAMP NOT NULL
);
CREATE TABLE IF NOT EXISTS "blobs" (
  key VARCHAR NOT NULL PRIMARY KEY,
  data BLOB NOT NULL,
  mime VARCHAR NOT NULL,
  size INTEGER NOT NULL,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  deleted_at TIMESTAMP
);
CREATE TABLE IF NOT EXISTS "peer_clocks" (
  peer VARCHAR NOT NULL,
  doc_id VARCHAR NOT NULL,
  remote_clock TIMESTAMP NOT NULL DEFAULT 0,
  pulled_remote_clock TIMESTAMP NOT NULL DEFAULT 0,
  pushed_clock TIMESTAMP NOT NULL DEFAULT 0,
  PRIMARY KEY (peer, doc_id)
);
CREATE TABLE IF NOT EXISTS "peer_blob_sync" (
  peer VARCHAR NOT NULL,
  blob_id VARCHAR NOT NULL,
  uploaded_at TIMESTAMP,
  PRIMARY KEY (peer, blob_id)
);
CREATE TABLE IF NOT EXISTS "idx_snapshots" (
  index_name TEXT NOT NULL PRIMARY KEY,
  data BLOB NOT NULL,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE IF NOT EXISTS "indexer_sync" (
  doc_id VARCHAR NOT NULL PRIMARY KEY,
  indexed_clock TIMESTAMP NOT NULL DEFAULT 0,
  indexer_version INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS "peer_clocks_doc_id" ON peer_clocks (doc_id);
CREATE INDEX IF NOT EXISTS "peer_blob_sync_peer" ON peer_blob_sync (peer);
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

/**
 * Primary doc enumeration: WS `space:load-doc-timestamps` returns every doc in
 * the workspace (pages, the root doc, and db$ sub-docs), filtered by the
 * caller's read permission. Falls back to reading the root doc's meta.pages if
 * the timestamps call fails.
 */
async function readAllDocIdsFromTimestamps(
  gql: GraphQLClient,
  workspaceId: string
): Promise<Set<string>> {
  const { endpoint, cookie, bearer } = await gql.getConnectionAuth();
  const socket = await connectWorkspaceSocket(wsUrlFromGraphQLEndpoint(endpoint), cookie, bearer);
  try {
    await joinWorkspace(socket, workspaceId);
    const timestamps = await loadDocTimestamps(socket, workspaceId);
    return new Set(Object.keys(timestamps));
  } finally {
    socket.disconnect();
  }
}

async function downloadBinary(url: string, headers: Record<string, string>): Promise<{ data: Uint8Array; mimeType: string }> {
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
    const mimeType = (res.headers.get("content-type") ?? "application/octet-stream").split(";")[0].trim();
    return { data: new Uint8Array(buffer), mimeType };
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

    // 1. Enumerate documents. Primary source: WS `space:load-doc-timestamps`,
    // which includes workspace sub-docs (db$...) that GraphQL workspace.docs
    // omits. We also probe known sub-doc guids (explorerIcon, docProperties,
    // folders, ...) in both legacy and current naming forms so icons and other
    // workspace metadata are never dropped from the backup.
    const docIds = new Set<string>();
    try {
      const timestampIds = await readAllDocIdsFromTimestamps(gql, workspaceId);
      for (const docId of timestampIds) {
        docIds.add(docId);
      }
    } catch (err) {
      // Fallback 1: GraphQL workspace.docs (needs Workspace.Users.Manage).
      try {
        const data = await gql.request<DocListResponse>(DOC_LIST_QUERY, { workspaceId });
        for (const edge of data.workspace?.docs?.edges ?? []) {
          const docId = edge?.node?.id;
          if (typeof docId === "string" && docId.length > 0) {
            docIds.add(docId);
          }
        }
      } catch (gqlErr) {
        // Fallback 2: workspace root doc meta.pages.
        try {
          const wsDocIds = await readWorkspacePageIdsFromRootDoc(gql, workspaceId);
          for (const docId of wsDocIds) {
            docIds.add(docId);
          }
        } catch (fallbackErr) {
          throw new Error(
            `Failed to enumerate workspace documents: ${(err as Error).message} ` +
              `(GraphQL fallback: ${(gqlErr as Error).message}; meta.pages fallback: ${(fallbackErr as Error).message})`
          );
        }
      }
    }
    docIds.add(workspaceId); // workspace root doc (meta: pages, tags, ...)
    for (const candidate of workspaceSubDocCandidates(workspaceId)) {
      docIds.add(candidate); // probed below; missing ones are skipped at download
    }

    // 2. Download each document's merged Yjs state. Probed sub-doc guids that
    // do not exist on the server (e.g. a workspace that never created an
    // explorerIcon sub-doc) are skipped silently instead of being reported as
    // download failures.
    const probedCandidates = new Set(workspaceSubDocCandidates(workspaceId));
    const docBins = new Map<string, Uint8Array>();
    const docFailures: Array<{ docId: string; error: string }> = [];
    const skippedMissingSubDocs: string[] = [];
    await mapLimit([...docIds], DOWNLOAD_CONCURRENCY, async (docId) => {
      try {
        const { data } = await downloadBinary(
          `${baseUrl}/api/workspaces/${wsPart}/docs/${encodeURIComponent(docId)}`,
          headers
        );
        docBins.set(docId, data);
      } catch (err) {
        if (probedCandidates.has(docId)) {
          // Probe only: this sub-doc was never created in this workspace.
          skippedMissingSubDocs.push(docId);
        } else {
          docFailures.push({ docId, error: (err as Error).message });
        }
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
    const blobData = new Map<string, { data: Uint8Array; mimeType: string }>();
    const blobFailures: Array<{ key: string; error: string }> = [];
    if (includeBlobs && blobKeys.size > 0) {
      await mapLimit([...blobKeys], DOWNLOAD_CONCURRENCY, async (key) => {
        try {
          const result = await downloadBinary(
            `${baseUrl}/api/workspaces/${wsPart}/blobs/${encodeURIComponent(key)}`,
            headers
          );
          blobData.set(key, result);
        } catch (err) {
          blobFailures.push({ key, error: (err as Error).message });
        }
      });
    }

    // 5. Write the nbstore-v2 SQLite database.
    // Priority: explicit outputPath > AFFINE_EXPORT_DIR > system temp dir.
    const outputPath =
      params.outputPath?.trim() ||
      (defaults.exportDir
        ? path.join(defaults.exportDir, `affine-mcp-export-${workspaceId}-${Date.now()}.affine`)
        : path.join(os.tmpdir(), `affine-mcp-export-${workspaceId}-${Date.now()}.affine`));
    fs.mkdirSync(path.dirname(outputPath), { recursive: true });

    // The output file may already exist from a previous run (e.g. the same
    // explicit outputPath was used before). Remove it so we start from a fresh
    // database: otherwise `CREATE TABLE IF NOT EXISTS` is a no-op and the
    // subsequent INSERTs collide with the leftover primary keys
    // (snapshots.doc_id / _sqlx_migrations.version) → UNIQUE constraint failure.
    if (fs.existsSync(outputPath)) {
      fs.rmSync(outputPath, { force: true });
    }

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
      db.exec(NBSTORE_V2_SCHEMA);

      // Record all 4 sqlx migrations as applied so the desktop's migrate()
      // (run inside set_space_id -> connect()) becomes a no-op instead of
      // re-running init_v2 and failing on the already-existing tables.
      const insertMigration = db.prepare(
        "INSERT INTO _sqlx_migrations (version, description, installed_on, success, checksum, execution_time) VALUES (?, ?, ?, 1, ?, 0)"
      );
      const migrationTime = new Date().toISOString().replace("T", " ").slice(0, 23);
      for (const m of SQLX_MIGRATIONS) {
        insertMigration.run(m.version, m.description, migrationTime, Buffer.from(m.checksumHex, "hex"));
      }

      // meta.space_id is the workspace id; the import's setSpaceId() rewrites
      // it (plus the root doc's doc_id in snapshots/updates/clocks) to the new
      // workspace id on the desktop.
      db.prepare("INSERT INTO meta (space_id) VALUES (?)").run(workspaceId);

      // snapshots: one row per doc. doc_id for the root doc is the workspace
      // id (setSpaceId rewrites it); sub-docs use the desktop form db$<table>
      // (no workspace id) so WorkspaceDB finds db$explorerIcon / db$folders /
      // db$docProperties after import.
      const now = new Date().toISOString().replace("T", " ").slice(0, 23);
      const insertSnapshot = db.prepare(
        "INSERT INTO snapshots (doc_id, data, created_at, updated_at) VALUES (?, ?, ?, ?)"
      );
      const insertClock = db.prepare("INSERT INTO clocks (doc_id, timestamp) VALUES (?, ?)");
      for (const [docId, bin] of docBins) {
        const dbDocId = docId === workspaceId ? workspaceId : toDesktopSubDocId(docId, workspaceId);
        insertSnapshot.run(dbDocId, bin, now, now);
        insertClock.run(dbDocId, now);
      }

      // updates: keep empty (snapshots carry the full state; AFFiNE reads the
      // snapshot first and treats missing updates as "no pending changes").

      // blobs: full metadata so the desktop can resolve attachment mime/size.
      const insertBlob = db.prepare(
        "INSERT INTO blobs (key, data, mime, size, created_at, deleted_at) VALUES (?, ?, ?, ?, ?, NULL)"
      );
      for (const [key, { data, mimeType }] of blobData) {
        insertBlob.run(key, data, mimeType || "application/octet-stream", data.byteLength, now);
      }
    } finally {
      db.close();
    }

    const totalDocBytes = [...docBins.values()].reduce((sum, bin) => sum + bin.byteLength, 0);
    const totalBlobBytes = [...blobData.values()].reduce((sum, data) => sum + data.data.byteLength, 0);

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
      skippedMissingSubDocs,
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
