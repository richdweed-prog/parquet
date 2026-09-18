import { parquetWriteBuffer } from "hyparquet-writer";
import { snappyCompress } from "hysnappy";

type WorkerRequest = { file: File; compress: boolean };
type WorkerResponse = { type: "progress"; progress: number } | { type: "done"; rows: string[]; removed: number; buffer: ArrayBuffer } | { type: "error"; message: string };

const workerScope = self as unknown as { postMessage: (message: WorkerResponse, transfer?: Transferable[]) => void };
const post = (message: WorkerResponse, transfer?: Transferable[]) => workerScope.postMessage(message, transfer);
function cleanLine(line: string) {
  const normalized = line.trim();
  if (!normalized || !/[A-Za-zÀ-ÿ0-9]/.test(normalized)) return null;
  const noiseOnly = normalized.toLocaleLowerCase().replace(/[^a-z0-9]+/g, " ").trim();
  if (noiseOnly && noiseOnly.split(/\s+/).every((token) => ["3d", "async", "etc"].includes(token))) return null;
  return normalized;
}

self.onmessage = async (event: MessageEvent<WorkerRequest>) => {
  try {
    const { file, compress } = event.data;
    const decoder = new TextDecoder();
    const reader = file.stream().getReader();
    const seen = new Set<string>();
    const rows: string[] = [];
    let pending = "";
    let received = 0;
    let totalLines = 0;

    while (true) {
      const chunk = await reader.read();
      if (chunk.done) break;
      pending += decoder.decode(chunk.value, { stream: true });
      const lines = pending.split(/\r?\n|\r/);
      pending = lines.pop() ?? "";
      for (const line of lines) {
        totalLines += 1;
        const cleaned = cleanLine(line);
        if (cleaned !== null && !seen.has(cleaned)) {
          seen.add(cleaned);
          rows.push(cleaned);
        } else {
          // Only exact duplicate lines are removed. Whitespace and casing are untouched.
        }
      }
      received += chunk.value.byteLength;
      post({ type: "progress", progress: file.size ? Math.min(92, Math.round((received / file.size) * 92)) : 92 });
    }

    pending += decoder.decode();
    if (pending.length > 0 || rows.length === 0) {
      totalLines += 1;
      const cleaned = cleanLine(pending);
      if (cleaned !== null && !seen.has(cleaned)) rows.push(cleaned);
    }

    const buffer = parquetWriteBuffer({
      codec: compress ? "SNAPPY" : "UNCOMPRESSED",
      compressors: compress ? { SNAPPY: snappyCompress } : undefined,
      columnData: [{ name: "linha", type: "STRING", data: rows }],
      kvMetadata: [
        { key: "origem", value: file.name },
        { key: "limpeza", value: "vazios, símbolos isolados, ruídos 3d/async/etc e duplicatas removidos; links preservados; ordem preservada" },
      ],
    });
    post({ type: "progress", progress: 100 });
    post({ type: "done", rows, removed: Math.max(0, totalLines - rows.length), buffer }, [buffer]);
  } catch (error) {
    post({ type: "error", message: error instanceof Error ? error.message : "Não foi possível processar o arquivo." });
  }
};

export {};
