import { createReadStream, createWriteStream } from "node:fs";
import { appendFile, mkdir, rm, stat } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { randomUUID } from "node:crypto";
import { once } from "node:events";
import { pipeline } from "node:stream/promises";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { StringDecoder } from "node:string_decoder";
import type { Request, Response } from "express";
import { fileWriter, parquetWriteRows } from "hyparquet-writer";
import { snappyCompress } from "hysnappy";
import { asyncBufferFromFile, parquetRead } from "hyparquet";

const execFileAsync = promisify(execFile);
const BUCKETS = 256;
const MAX_UPLOAD_BYTES = 30 * 1024 * 1024 * 1024;
const noiseWords = new Set(["3d", "async", "etc"]);

async function* readLinesFast(filePath: string) {
  const input = createReadStream(filePath, { highWaterMark: 8 * 1024 * 1024 });
  const decoder = new StringDecoder("utf8");
  let pending = "";
  for await (const chunk of input) {
    pending += decoder.write(chunk as Buffer);
    let newline;
    while ((newline = pending.indexOf("\n")) >= 0) {
      const line = pending.slice(0, newline);
      pending = pending.slice(newline + 1);
      yield line.endsWith("\r") ? line.slice(0, -1) : line;
    }
  }
  pending += decoder.end();
  if (pending.length) yield pending.endsWith("\r") ? pending.slice(0, -1) : pending;
}
const uploadSessions = new Map<string, { root: string; inputPath: string; sourceName: string; received: number; files: Map<string, { path: string; received: number }> }>();

type ConversionStats = { inputLines: number; outputLines: number; removedLines: number };

function safeFilename(value: string | undefined) {
  let decoded = value ?? "arquivo.txt";
  try { decoded = decodeURIComponent(decoded); } catch { /* keep the raw header */ }
  const clean = decoded.replace(/[^a-zA-Z0-9._-]+/g, "_");
  return /\.(txt|parquet)$/i.test(clean) ? clean : `${clean}.txt`;
}

function cleanLine(line: string) {
  const normalized = line.trim();
  if (!normalized || !/[A-Za-zÀ-ÿ0-9]/.test(normalized)) return null;
  const noiseOnly = normalized.toLocaleLowerCase().replace(/[^a-z0-9]+/g, " ").trim();
  if (noiseOnly && noiseOnly.split(/\s+/).every((token) => noiseWords.has(token))) return null;
  return normalized;
}

function hashLine(value: string) {
  let hash = 2166136261;
  for (let index = 0; index < value.length; index += 1) {
    hash ^= value.charCodeAt(index);
    hash = Math.imul(hash, 16777619);
  }
  return (hash >>> 0) % BUCKETS;
}

async function saveRequest(req: Request, inputPath: string) {
  const output = createWriteStream(inputPath, { flags: "wx" });
  let received = 0;
  req.on("data", (chunk: Buffer) => {
    received += chunk.byteLength;
    if (received > MAX_UPLOAD_BYTES) req.destroy(new Error("Arquivo acima do limite de 30 GB."));
  });
  req.pipe(output);
  await once(output, "finish");
  return received;
}

async function partitionInput(inputPath: string, root: string) {
  const handles = Array.from({ length: BUCKETS }, (_, index) => createWriteStream(join(root, `bucket-${index}.txt`), { flags: "wx", highWaterMark: 1024 * 1024 }));
  let inputLines = 0;
  let keptLines = 0;
  try {
    for await (const raw of readLinesFast(inputPath)) {
      inputLines += 1;
      const line = cleanLine(String(raw));
      if (line === null) continue;
      const handle = handles[hashLine(line)];
      if (!handle.write(`${inputLines}\t${line}\n`)) await once(handle, "drain");
      keptLines += 1;
    }
  } finally {
    await Promise.all(handles.map((handle) => new Promise<void>((resolve, reject) => {
      handle.once("finish", resolve);
      handle.once("error", reject);
      handle.end();
    })));
  }
  return { inputLines, keptLines };
}

async function fastUniqueInput(inputPath: string, uniquePath: string) {
  const unique = createWriteStream(uniquePath, { flags: "wx" });
  const seen = new Set<string>();
  let inputLines = 0;
  let outputLines = 0;
  try {
    for await (const raw of readLinesFast(inputPath)) {
      inputLines += 1;
      const line = cleanLine(String(raw));
      if (line === null || seen.has(line)) continue;
      seen.add(line);
      if (!unique.write(`${inputLines}\t${line}\n`)) await once(unique, "drain");
      outputLines += 1;
    }
  } finally {
    await new Promise<void>((resolve, reject) => {
      unique.once("finish", resolve);
      unique.once("error", reject);
      unique.end();
    });
  }
  return { inputLines, outputLines };
}

async function writeUniqueRows(root: string, uniquePath: string) {
  const unique = createWriteStream(uniquePath, { flags: "wx", highWaterMark: 1024 * 1024 });
  let outputLines = 0;
  try {
    for (let index = 0; index < BUCKETS; index += 1) {
      const bucketPath = join(root, `bucket-${index}.txt`);
      const sortedBucketPath = join(root, `bucket-${index}.sorted.txt`);
      if (process.platform === "win32") {
        // Windows `sort.exe` has a different syntax and cannot sort by fields.
        // Deduplicate one partition at a time without invoking a shell command.
        const seen = new Set<string>();
        for await (const raw of readLinesFast(bucketPath)) {
          const separator = String(raw).indexOf("\t");
          if (separator < 0) continue;
          const lineNumber = String(raw).slice(0, separator);
          const line = String(raw).slice(separator + 1);
          if (seen.has(line)) continue;
          seen.add(line);
          if (!unique.write(`${lineNumber}\t${line}\n`)) await once(unique, "drain");
          outputLines += 1;
        }
      } else {
        await execFileAsync("sort", ["-t", "\t", "-k2,2", "-k1,1n", bucketPath, "-o", sortedBucketPath], { maxBuffer: 1024 * 1024 });
        let previousLine = "";
        for await (const raw of readLinesFast(sortedBucketPath)) {
          const separator = String(raw).indexOf("\t");
          if (separator < 0) continue;
          const lineNumber = String(raw).slice(0, separator);
          const line = String(raw).slice(separator + 1);
          if (line === previousLine) continue;
          previousLine = line;
          if (!unique.write(`${lineNumber}\t${line}\n`)) await once(unique, "drain");
          outputLines += 1;
        }
      }
    }
  } finally {
    await new Promise<void>((resolve, reject) => {
      unique.once("finish", resolve);
      unique.once("error", reject);
      unique.end();
    });
  }
  return outputLines;
}

async function sortRows(uniquePath: string, sortedPath: string) {
  await execFileAsync("sort", ["-n", "-t", "\t", "-k1,1", uniquePath, "-o", sortedPath], { maxBuffer: 1024 * 1024 });
}

async function createParquet(sortedPath: string, parquetPath: string, sourceName: string) {
  async function* rows() {
    for await (const raw of readLinesFast(sortedPath)) {
      const separator = String(raw).indexOf("\t");
      if (separator >= 0) yield { linha: String(raw).slice(separator + 1) };
    }
  }
  await parquetWriteRows({
    writer: fileWriter(parquetPath),
    rows: rows(),
    columns: [{ name: "linha", type: "STRING" }],
    codec: "SNAPPY",
    compressors: { SNAPPY: snappyCompress },
    rowGroupSize: [10_000, 100_000],
    kvMetadata: [
      { key: "origem", value: sourceName },
      { key: "limpeza", value: "vazios, símbolos isolados, 3d/async/etc e duplicatas removidos; links preservados" },
    ],
  });
}

async function parquetToText(inputPath: string, textPath: string) {
  const output = createWriteStream(textPath, { flags: "wx", highWaterMark: 1024 * 1024 });
  let lines = 0;
  let pending = Promise.resolve();
  const file = await asyncBufferFromFile(inputPath);
  await parquetRead({
    file,
    columns: ["linha"],
    onChunk: ({ columnData }: { columnData: any }) => {
      const values = Array.from(columnData as Iterable<unknown>).map((value) => `${value ?? ""}\n`).join("");
      lines += (columnData as { length: number }).length;
      pending = pending.then(async () => {
        if (!output.write(values)) await once(output, "drain");
      });
    },
  });
  await pending;
  await new Promise<void>((resolve, reject) => { output.once("finish", resolve); output.once("error", reject); output.end(); });
  return lines;
}

async function convertSavedInput(inputPath: string, root: string, sourceName: string, res: Response) {
  const uniquePath = join(root, "unique.txt");
  const sortedPath = join(root, "sorted.txt");
  const parquetPath = join(root, "output.parquet");
  try {
    const normalizedInputPath = sourceName.toLowerCase().endsWith(".parquet") ? join(root, "parquet-as-text.txt") : inputPath;
    if (normalizedInputPath !== inputPath) await parquetToText(inputPath, normalizedInputPath);
    const isWindows = process.platform === "win32";
    let inputLines = 0;
    let outputLines = 0;
    const partitioned = await partitionInput(normalizedInputPath, root);
    inputLines = partitioned.inputLines;
    outputLines = await writeUniqueRows(root, uniquePath);
    const parquetInputPath = isWindows ? uniquePath : sortedPath;
    if (!isWindows) await sortRows(uniquePath, sortedPath);
    await createParquet(parquetInputPath, parquetPath, sourceName);
    const outputStats = await stat(parquetPath);
    const stats: ConversionStats = {
      inputLines,
      outputLines,
      removedLines: Math.max(0, inputLines - outputLines),
    };
    res.status(200);
    res.setHeader("Content-Type", "application/vnd.apache.parquet");
    res.setHeader("Content-Disposition", `attachment; filename="output.parquet"`);
    res.setHeader("Content-Length", outputStats.size);
    res.setHeader("X-Input-Lines", String(stats.inputLines));
    res.setHeader("X-Output-Lines", String(stats.outputLines));
    res.setHeader("X-Removed-Lines", String(stats.removedLines));
    createReadStream(parquetPath).pipe(res);
    await once(res, "finish");
  } catch (error) {
    if (!res.headersSent) res.status(500).json({ message: error instanceof Error ? error.message : "Falha ao converter o arquivo." });
  } finally {
    await rm(root, { recursive: true, force: true });
  }
}

export async function convertTxtRequest(req: Request, res: Response) {
  const root = join(tmpdir(), `txt-index-${randomUUID()}`);
  await mkdir(root, { recursive: true });
  const inputPath = join(root, "input.txt");
  const sourceName = safeFilename(req.header("x-file-name") ?? req.header("x-filename"));
  try {
    const contentLength = Number(req.header("content-length") ?? 0);
    if (contentLength > MAX_UPLOAD_BYTES) {
      res.status(413).json({ message: "Arquivo acima do limite de 30 GB." });
      return;
    }
    await saveRequest(req, inputPath);
    await convertSavedInput(inputPath, root, sourceName, res);
  } catch (error) {
    if (!res.headersSent) res.status(500).json({ message: error instanceof Error ? error.message : "Falha ao converter o arquivo." });
  }
}

export async function startChunkedUpload(req: Request, res: Response) {
  const id = randomUUID();
  const root = join(tmpdir(), `txt-index-upload-${id}`);
  await mkdir(root, { recursive: true });
  const inputPath = join(root, "input.txt");
  uploadSessions.set(id, { root, inputPath, sourceName: safeFilename(req.body?.fileName), received: 0, files: new Map() });
  res.json({ id });
}

export async function appendUploadChunk(req: Request, res: Response) {
  const id = req.params.id;
  const session = uploadSessions.get(id);
  if (!session) { res.status(404).json({ message: "Sessão de upload não encontrada." }); return; }
  const sourceName = safeFilename(req.header("x-source-name") ?? session.sourceName);
  let source = session.files.get(sourceName);
  if (!source) {
    source = { path: join(session.root, `source-${session.files.size}.bin`), received: 0 };
    session.files.set(sourceName, source);
  }
  const chunk = Buffer.isBuffer(req.body) ? req.body : Buffer.from(req.body ?? "");
  session.received += chunk.byteLength;
  source.received += chunk.byteLength;
  if (session.received > MAX_UPLOAD_BYTES) {
    uploadSessions.delete(id);
    await rm(session.root, { recursive: true, force: true });
    res.status(413).json({ message: "Arquivo acima do limite de 30 GB." });
    return;
  }
  await appendFile(source.path, chunk);
  res.json({ received: session.received });
}

export async function finishChunkedUpload(req: Request, res: Response) {
  const id = req.params.id;
  const session = uploadSessions.get(id);
  if (!session) { res.status(404).json({ message: "Sessão de upload não encontrada." }); return; }
  uploadSessions.delete(id);
  if (session.files.size <= 1) {
    const only = session.files.values().next().value;
    await convertSavedInput(only?.path ?? session.inputPath, session.root, session.sourceName, res);
    return;
  }
  const combinedPath = join(session.root, "combined.txt");
  const combined = createWriteStream(combinedPath, { flags: "wx" });
  for (const [sourceName, source] of Array.from(session.files.entries())) {
    const textPath = sourceName.toLowerCase().endsWith(".parquet") ? join(session.root, `normalized-${source.received}.txt`) : source.path;
    if (textPath !== source.path) await parquetToText(source.path, textPath);
    await pipeline(createReadStream(textPath), combined, { end: false });
    if (!combined.write("\n")) await once(combined, "drain");
  }
  await new Promise<void>((resolve, reject) => { combined.once("finish", resolve); combined.once("error", reject); combined.end(); });
  await convertSavedInput(combinedPath, session.root, "super.txt", res);
}
