// ──────────────────────────────────────────────
// Routes: Knowledge Sources (file uploads for Knowledge Retrieval agent)
// ──────────────────────────────────────────────
import type { FastifyInstance } from "fastify";
import { join, extname, basename } from "path";
import { mkdir, readFile, unlink, writeFile, stat } from "fs/promises";
import { createWriteStream, existsSync, mkdirSync, readFileSync } from "fs";
import { pipeline } from "stream/promises";
import { nanoid } from "nanoid";
import { DATA_DIR } from "../utils/data-dir.js";

const SOURCES_DIR = join(DATA_DIR, "knowledge-sources");
const META_FILE = join(SOURCES_DIR, "meta.json");

// Supported text-based formats (read as UTF-8)
const TEXT_EXTS = new Set([".txt", ".md", ".csv", ".json", ".xml", ".html", ".htm", ".log", ".yaml", ".yml", ".tsv"]);
// PDF support via pdf-parse
const PDF_EXTS = new Set([".pdf"]);
const ALLOWED_EXTS = new Set([...TEXT_EXTS, ...PDF_EXTS]);

interface SourceMeta {
  id: string;
  originalName: string;
  filename: string;
  size: number;
  uploadedAt: string;
}

type MetaStore = Record<string, SourceMeta>;

// In-process cache of extracted file text, keyed by source id. An entry is valid
// only while (size, uploadedAt) match the current meta, so a re-upload (which
// changes both) or a delete invalidates it. Avoids re-reading + re-parsing the
// file (a full PDF parse for PDFs) on every generation turn.
interface CacheEntry {
  size: number;
  uploadedAt: string;
  text: string;
}
const textCache = new Map<string, CacheEntry>();

function ensureDir() {
  if (!existsSync(SOURCES_DIR)) {
    mkdirSync(SOURCES_DIR, { recursive: true });
  }
}

function readMeta(): MetaStore {
  if (!existsSync(META_FILE)) return {};
  try {
    return JSON.parse(readFileSync(META_FILE, "utf-8"));
  } catch {
    return {};
  }
}

// Simple in-process queue to serialize writes to META_FILE and avoid
// concurrent write operations that could corrupt or overwrite metadata.
let metaWriteChain: Promise<void> = Promise.resolve();

type MetaStoreUpdater = (current: MetaStore) => MetaStore | Promise<MetaStore>;

async function writeMeta(mutator: MetaStoreUpdater) {
  // Re-read meta INSIDE the serialized critical section so each mutation observes
  // prior committed state — a pre-captured snapshot would let two overlapping
  // upload/delete calls each persist their own stale view (lost update / TOCTOU).
  const apply = async () => {
    const next = await mutator(readMeta());
    await writeFile(META_FILE, JSON.stringify(next, null, 2), "utf-8");
  };
  // Run the mutation whether the previous link resolved or rejected, but keep
  // propagating failures to this call's awaiter.
  metaWriteChain = metaWriteChain.then(apply, apply);

  await metaWriteChain;
}

/**
 * Look up a knowledge-source file by its ID.
 * Returns the resolved file path and original name, or null if not found.
 */
export function getSourceFilePath(
  id: string,
): { filePath: string; originalName: string; size: number; uploadedAt: string } | null {
  const meta = readMeta();
  const entry = meta[id];
  if (!entry) return null;
  return {
    filePath: join(SOURCES_DIR, entry.filename),
    originalName: entry.originalName,
    size: entry.size,
    uploadedAt: entry.uploadedAt,
  };
}

/**
 * Extract plain text from a file based on its extension.
 *
 * When `fileId` and `metadata` are supplied, the result is cached and reused
 * across calls while the file's (size, uploadedAt) are unchanged, so the
 * generation pipeline does not re-read/re-parse the same source every turn.
 */
export async function extractFileText(
  filePath: string,
  fileId?: string,
  metadata?: { size: number; uploadedAt: string },
): Promise<string> {
  // Ensure the resolved path is within SOURCES_DIR (defense-in-depth)
  const { resolve, sep } = await import("path");
  const resolved = resolve(filePath);
  const root = resolve(SOURCES_DIR);
  if (resolved !== root && !resolved.startsWith(root + sep)) {
    return "";
  }

  if (fileId && metadata) {
    const cached = textCache.get(fileId);
    if (cached && cached.size === metadata.size && cached.uploadedAt === metadata.uploadedAt) {
      return cached.text;
    }
  }

  const ext = extname(filePath).toLowerCase();
  let text = "";

  if (TEXT_EXTS.has(ext)) {
    text = await readFile(filePath, "utf-8");
  } else if (PDF_EXTS.has(ext)) {
    try {
      const { PDFParse } = await import("pdf-parse");
      const buf = await readFile(filePath);
      const pdf = new PDFParse({ data: new Uint8Array(buf) });
      const result = await pdf.getText();
      await pdf.destroy();
      text = result.text;
    } catch {
      text = "[PDF text extraction failed]";
    }
  }

  if (fileId && metadata) {
    textCache.set(fileId, { size: metadata.size, uploadedAt: metadata.uploadedAt, text });
  }

  return text;
}

export async function knowledgeSourcesRoutes(app: FastifyInstance) {
  // ── List all uploaded sources ──
  app.get("/", async () => {
    ensureDir();
    const meta = readMeta();
    return Object.values(meta).sort((a, b) => new Date(b.uploadedAt).getTime() - new Date(a.uploadedAt).getTime());
  });

  // ── Upload a new source file ──
  app.post("/upload", async (req, reply) => {
    await mkdir(SOURCES_DIR, { recursive: true });
    const data = await req.file();
    if (!data) {
      return reply.status(400).send({ error: "No file uploaded" });
    }

    const ext = extname(data.filename).toLowerCase();
    if (!ALLOWED_EXTS.has(ext)) {
      return reply.status(400).send({
        error: `Unsupported file type: ${ext}. Supported: ${[...ALLOWED_EXTS].join(", ")}`,
      });
    }

    const id = nanoid();
    const filename = `${id}${ext}`;
    const filePath = join(SOURCES_DIR, filename);

    await pipeline(data.file, createWriteStream(filePath));

    const fileInfo = await stat(filePath);
    const entry: SourceMeta = {
      id,
      originalName: basename(data.filename),
      filename,
      size: fileInfo.size,
      uploadedAt: new Date().toISOString(),
    };
    await writeMeta((current) => {
      current[id] = entry;
      return current;
    });
    // A re-upload reuses the id; drop any stale extracted text for it.
    textCache.delete(id);

    return entry;
  });

  // ── Delete a source file ──
  app.delete<{ Params: { id: string } }>("/:id", async (req, reply) => {
    const { id } = req.params;
    const meta = readMeta();
    const entry = meta[id];
    if (!entry) {
      return reply.status(404).send({ error: "Source not found" });
    }

    const filePath = join(SOURCES_DIR, entry.filename);
    try {
      await unlink(filePath);
    } catch {
      /* file may already be gone */
    }
    await writeMeta((current) => {
      delete current[id];
      return current;
    });
    textCache.delete(id);
    return { success: true };
  });

  // ── Get text content of a source (for preview / debugging) ──
  app.get<{ Params: { id: string } }>("/:id/text", async (req, reply) => {
    const { id } = req.params;
    const meta = readMeta();
    const entry = meta[id];
    if (!entry) {
      return reply.status(404).send({ error: "Source not found" });
    }

    const filePath = join(SOURCES_DIR, entry.filename);
    if (!existsSync(filePath)) {
      return reply.status(404).send({ error: "File not found on disk" });
    }

    const text = await extractFileText(filePath, id, { size: entry.size, uploadedAt: entry.uploadedAt });
    return { id, originalName: entry.originalName, text };
  });
}
