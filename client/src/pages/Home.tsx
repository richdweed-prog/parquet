import { useCallback, useMemo, useRef, useState } from "react";
import {
  ArrowDownToLine,
  Check,
  Copy,
  FileText,
  FolderOpen,
  Grip,
  ListChecks,
  LoaderCircle,
  Sparkles,
  Trash2,
  UploadCloud,
  X,
} from "lucide-react";
import { PROFILE_PHOTO_DATA_URI } from "@/profilePhoto";

type OutputFile = {
  id: string;
  sourceName: string;
  fileName: string;
  rowCount: number;
  removed: number;
  bytes: number;
  blob?: Blob;
};

const alphabeticName = (index: number) => {
  if (index < 26) return String.fromCharCode(65 + index);
  const pairIndex = index - 26;
  const first = String.fromCharCode(65 + (pairIndex % 26));
  const second = String.fromCharCode(97 + ((pairIndex + 1) % 26));
  const cycle = Math.floor(pairIndex / 26);
  return cycle === 0 ? `${first}${second}` : `${first}${second}${cycle + 1}`;
};

const NAME_SEQUENCE_KEY = "txt-index-next-file-number";

const reserveFileNumber = () => {
  const saved = Number.parseInt(localStorage.getItem(NAME_SEQUENCE_KEY) ?? "0", 10);
  const next = Number.isFinite(saved) && saved >= 0 ? saved : 0;
  localStorage.setItem(NAME_SEQUENCE_KEY, String(next + 1));
  return next;
};

const formatBytes = (bytes: number) => {
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
  return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
};

function processWithServer(file: File, onProgress: (progress: number) => void) {
  return (async () => {
    const start = await fetch("/api/upload/start", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ fileName: file.name }),
    });
    if (!start.ok) throw new Error((await start.text()) || `Falha ao iniciar upload (HTTP ${start.status}).`);
    const { id } = await start.json() as { id: string };
    const chunkSize = 8 * 1024 * 1024;
    for (let offset = 0; offset < file.size; offset += chunkSize) {
      const chunk = file.slice(offset, Math.min(offset + chunkSize, file.size));
      const response = await fetch(`/api/upload/chunk/${id}`, {
        method: "POST",
        headers: { "Content-Type": "application/octet-stream" },
        body: chunk,
      });
      if (!response.ok) throw new Error((await response.text()) || "Falha ao enviar um bloco do arquivo.");
      onProgress(Math.min(90, Math.round(((offset + chunk.size) / file.size) * 90)));
    }
    const finished = await fetch(`/api/upload/finish/${id}`, { method: "POST" });
    if (!finished.ok) throw new Error((await finished.text()) || `Falha ao converter o arquivo (HTTP ${finished.status}).`);
    onProgress(100);
    return {
      rowCount: Number(finished.headers.get("X-Output-Lines") ?? 0),
      removed: Number(finished.headers.get("X-Removed-Lines") ?? 0),
      blob: await finished.blob(),
    };
  })();
}

async function processBatchWithServer(files: File[], onProgress: (progress: number) => void) {
  const start = await fetch("/api/upload/start", { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ fileName: "super.txt" }) });
  if (!start.ok) throw new Error((await start.text()) || "Não foi possível iniciar o upload consolidado.");
  const { id } = await start.json() as { id: string };
  const chunkSize = 8 * 1024 * 1024;
  const total = files.reduce((sum, file) => sum + file.size, 0);
  let sent = 0;
  for (const file of files) for (let offset = 0; offset < file.size; offset += chunkSize) {
    const chunk = file.slice(offset, Math.min(offset + chunkSize, file.size));
    const response = await fetch(`/api/upload/chunk/${id}`, { method: "POST", headers: { "Content-Type": "application/octet-stream", "X-Source-Name": encodeURIComponent(file.name) }, body: chunk });
    if (!response.ok) throw new Error((await response.text()) || "Falha ao enviar um bloco.");
    sent += chunk.size;
    onProgress(Math.min(90, Math.round((sent / total) * 90)));
  }
  const finished = await fetch(`/api/upload/finish/${id}`, { method: "POST" });
  if (!finished.ok) throw new Error((await finished.text()) || "Falha ao criar o super.parquet.");
  onProgress(100);
  return { rowCount: Number(finished.headers.get("X-Output-Lines") ?? 0), removed: Number(finished.headers.get("X-Removed-Lines") ?? 0), blob: await finished.blob() };
}

export default function Home() {
  const inputRef = useRef<HTMLInputElement>(null);
  const [outputs, setOutputs] = useState<OutputFile[]>([]);
  const [paste, setPaste] = useState("");
  const [isDragging, setIsDragging] = useState(false);
  const [isProcessing, setIsProcessing] = useState(false);
  const [progress, setProgress] = useState(0);
  const [notice, setNotice] = useState("");
  const [consolidate, setConsolidate] = useState(false);

  const totals = useMemo(() => ({
    files: outputs.length,
    rows: outputs.reduce((sum, item) => sum + item.rowCount, 0),
    removed: outputs.reduce((sum, item) => sum + item.removed, 0),
  }), [outputs]);

  const processEntries = useCallback(async (entries: File[]) => {
    if (!entries.length) return;
    setIsProcessing(true);
    setNotice("");
    const next: OutputFile[] = [];
    try {
      const normalizedEntries = entries;
      if (consolidate) {
        const processed = await processBatchWithServer(normalizedEntries, setProgress);
        const fileNumber = reserveFileNumber();
        setOutputs([{ id: `super-${fileNumber}-${Date.now()}`, sourceName: `${normalizedEntries.length} arquivos consolidados`, fileName: "super.parquet", rowCount: processed.rowCount, removed: processed.removed, bytes: processed.blob.size, blob: processed.blob }]);
        setNotice("Super Parquet pronto para baixar.");
        return;
      }
      const jobs = normalizedEntries;
      for (const entry of jobs) {
        const processed = await processWithServer(entry, setProgress);
        const { rowCount, removed, blob } = processed;
        const fileNumber = reserveFileNumber();
        next.push({
          id: `${entry.name}-${fileNumber}-${Date.now()}`,
          sourceName: consolidate ? `${entries.length} TXT consolidados` : entry.name,
          fileName: consolidate ? "super.parquet" : `${alphabeticName(fileNumber)}.parquet`,
          rowCount,
          removed,
          bytes: blob.size,
          blob,
        });
      }
      setOutputs(next);
      setNotice(consolidate ? "Super Parquet pronto para baixar." : `${next.length} arquivo${next.length > 1 ? "s" : ""} pronto${next.length > 1 ? "s" : ""} para baixar.`);
    } catch (error) {
      setNotice(error instanceof Error ? error.message : "Não foi possível converter o arquivo.");
    } finally {
      setIsProcessing(false);
    }
  }, [consolidate]);

  const readFiles = useCallback(async (files: FileList | File[]) => {
    const txtFiles = Array.from(files).filter((file) => /\.(txt|parquet)$/i.test(file.name));
    if (!txtFiles.length) {
      setNotice("Selecione pelo menos um arquivo .TXT ou .PARQUET.");
      return;
    }
    await processEntries(txtFiles);
  }, [processEntries]);

  const handlePaste = async () => {
    if (!paste.trim() && paste.length === 0) {
      setNotice("Cole o conteúdo de um ou mais TXT na caixa antes de processar.");
      return;
    }
    await processEntries([new File([paste], "colado.txt", { type: "text/plain" })]);
  };

  const download = (item: OutputFile) => {
    if (!item.blob) return;
    const url = URL.createObjectURL(item.blob);
    const anchor = document.createElement("a");
    anchor.href = url;
    anchor.download = item.fileName;
    anchor.click();
    URL.revokeObjectURL(url);
  };

  const clearAll = () => {
    setOutputs([]);
    setPaste("");
    setNotice("");
  };

  const onDrop = async (event: React.DragEvent<HTMLDivElement>) => {
    event.preventDefault();
    setIsDragging(false);
    await readFiles(event.dataTransfer.files);
  };

  return (
    <main className="app-shell">
      <div className="ambient ambient-one" />
      <div className="ambient ambient-two" />
      <nav className="topbar">
        <a className="brand" href="#top" aria-label="TXT Index início"><span className="brand-bracket">[</span> TXT <span className="brand-mark">INDEX</span> <span className="brand-bracket">]</span></a>
        <div className="nav-links"><a href="#converter">CONVERTER</a><a href="#como-funciona">COMO FUNCIONA</a><a className="telegram" href="https://t.me/wedze_grupo" target="_blank" rel="noreferrer">TELEGRAM ↗</a></div>
      </nav>

      <section id="top" className="hero-grid">
        <div className="hero-copy">
          <div className="eyebrow"><span className="live-dot" /> UPLOAD EM STREAMING · MODO TURBO</div>
          <h1>TXT <span>→</span><br /><em>PARQUET</em></h1>
          <p className="hero-description">Cole ou arraste seus arquivos. O modo turbo converte TXT em Parquet e remove somente vazios, símbolos, ruídos 3d/async/etc e duplicatas.</p>
          <div className="hero-meta"><span><Sparkles size={14} /> rápido</span><span><ListChecks size={14} /> em lote</span><span><Check size={14} /> privado</span></div>
        </div>
        <div className="portrait-frame" aria-label="Identidade visual inspirada na página de referência">
          <div className="portrait-glow" />
          <img src={PROFILE_PHOTO_DATA_URI} alt="Foto do proprietário do TXT Index" />
          <div className="portrait-caption"><strong>TXT INDEX</strong><small>BY · WEDZE_GRUPO</small></div>
        </div>
      </section>

      <section id="converter" className="workspace">
        <div className="section-heading"><div><span className="section-kicker">01 / ENTRADA</span><h2>Solte seus TXT aqui</h2></div><div className="section-actions"><label className="consolidate-toggle"><input type="checkbox" checked={consolidate} onChange={(event) => setConsolidate(event.target.checked)} /><span>Consolidar em um super.parquet</span></label><span className="format-pill">.TXT → .PARQUET · até 30 GB</span></div></div>
        <div className="input-grid">
          <div className={`dropzone ${isDragging ? "is-dragging" : ""}`} onDragOver={(e) => { e.preventDefault(); setIsDragging(true); }} onDragLeave={() => setIsDragging(false)} onDrop={onDrop} onClick={() => inputRef.current?.click()} role="button" tabIndex={0} onKeyDown={(e) => { if (e.key === "Enter" || e.key === " ") inputRef.current?.click(); }}>
            <input ref={inputRef} type="file" accept=".txt,.parquet,text/plain,application/vnd.apache.parquet" multiple hidden onChange={(e) => e.target.files && readFiles(e.target.files)} />
            <div className="drop-icon"><UploadCloud size={26} /></div>
            <strong>Arraste um ou vários arquivos</strong>
            <span>ou clique para escolher o caminho no seu dispositivo</span>
            <div className="drop-foot"><FileText size={14} /> .TXT ou .PARQUET <span>·</span> múltiplos arquivos aceitos</div>
          </div>
          <div className="paste-panel">
            <div className="panel-label"><Copy size={14} /> OU COLE O CONTEÚDO</div>
            <textarea value={paste} onChange={(e) => setPaste(e.target.value)} placeholder="Cole aqui as linhas do seu TXT...\nCada linha vira um registro no Parquet." aria-label="Conteúdo TXT para converter" />
            <button className="primary-button" onClick={handlePaste} disabled={isProcessing}>{isProcessing ? <LoaderCircle className="spin" size={17} /> : <ArrowDownToLine size={17} />} {isProcessing ? `PROCESSANDO ${progress}%` : "PROCESSAR COLADO"}</button>
          </div>
        </div>
      </section>

      <section className="results-section">
        <div className="section-heading"><div><span className="section-kicker">02 / SAÍDA</span><h2>Arquivos limpos</h2></div>{outputs.length > 0 && <button className="clear-button" onClick={clearAll}><Trash2 size={14} /> limpar lista</button>}</div>
        {notice && <div className="notice"><Check size={15} /> {notice}</div>}
        {outputs.length === 0 ? <div className="empty-state"><Grip size={22} /><span>Envie um TXT para liberar o download</span><small>Depois do processamento, o botão <strong>baixar</strong> aparece ao lado de cada .parquet.</small><button className="download-button disabled-preview" disabled><ArrowDownToLine size={16} /> baixar Parquet</button></div> : <div className="results-list">{outputs.map((item, index) => <div className="result-row" key={item.id}><div className="file-letter">{alphabeticName(index)}</div><div className="result-main"><strong>{item.fileName}</strong><span>{item.sourceName} · {item.rowCount.toLocaleString("pt-BR")} linhas · {item.removed.toLocaleString("pt-BR")} linhas removidas</span></div><div className="result-size">{formatBytes(item.bytes)}</div><button className="download-button" onClick={() => download(item)}><ArrowDownToLine size={16} /> baixar</button></div>)}</div>}
        {outputs.length > 0 && <div className="summary"><span><strong>{totals.files}</strong> arquivos</span><span><strong>{totals.rows.toLocaleString("pt-BR")}</strong> linhas únicas</span><span><strong>{totals.removed.toLocaleString("pt-BR")}</strong> duplicatas removidas</span></div>}
      </section>

      <section id="como-funciona" className="how-section"><span className="section-kicker">03 / MÉTODO</span><div className="how-grid"><div><h2>Turbo sem travar.</h2><p>O upload vai em streaming para o servidor. A deduplicação é feita por partições no disco, sem guardar 1 GB inteiro na memória; depois o Parquet comprimido volta pronto para download.</p></div><div className="steps"><div><b>01</b><span>Enviar ou arrastar TXT</span></div><div><b>02</b><span>Processar em streaming</span></div><div><b>03</b><span>Baixar Parquet</span></div></div></div></section>
      <footer><span>TXT INDEX · DRW03</span><span>feito para limpar, organizar e seguir.</span></footer>
    </main>
  );
}
