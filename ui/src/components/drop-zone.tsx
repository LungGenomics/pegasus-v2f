import { useCallback, useRef, useState, type DragEvent } from "react";
import { Upload } from "lucide-react";

export function DropZone({
  onFile,
  onUrl,
  accept = ".csv,.tsv,.txt",
}: {
  onFile: (file: File) => void;
  onUrl: (url: string) => void;
  accept?: string;
}) {
  const [dragging, setDragging] = useState(false);
  const [urlInput, setUrlInput] = useState("");
  const fileRef = useRef<HTMLInputElement>(null);

  const handleDrop = useCallback(
    (e: DragEvent) => {
      e.preventDefault();
      setDragging(false);
      const file = e.dataTransfer.files[0];
      if (file) onFile(file);
    },
    [onFile],
  );

  function handleUrlSubmit() {
    const trimmed = urlInput.trim();
    if (trimmed) {
      onUrl(trimmed);
      setUrlInput("");
    }
  }

  return (
    <div
      className={`border-2 border-dashed rounded-box p-8 text-center transition-colors ${
        dragging ? "border-primary bg-primary/5" : "border-base-300"
      }`}
      onDragOver={(e) => {
        e.preventDefault();
        setDragging(true);
      }}
      onDragLeave={() => setDragging(false)}
      onDrop={handleDrop}
    >
      <Upload className="size-8 mx-auto mb-3 opacity-40" />
      <p className="mb-2 font-medium">
        Drop a CSV/TSV file here, or{" "}
        <button
          type="button"
          className="link link-primary"
          onClick={() => fileRef.current?.click()}
        >
          browse
        </button>
      </p>
      <input
        ref={fileRef}
        type="file"
        accept={accept}
        className="hidden"
        onChange={(e) => {
          const f = e.target.files?.[0];
          if (f) onFile(f);
        }}
      />
      <div className="divider text-xs">OR paste a Google Sheets URL</div>
      <div className="join w-full max-w-lg mx-auto">
        <input
          type="text"
          className="input input-bordered join-item grow"
          placeholder="https://docs.google.com/spreadsheets/d/..."
          value={urlInput}
          onChange={(e) => setUrlInput(e.target.value)}
          onKeyDown={(e) => e.key === "Enter" && handleUrlSubmit()}
        />
        <button
          type="button"
          className="btn btn-primary join-item"
          onClick={handleUrlSubmit}
          disabled={!urlInput.trim()}
        >
          Preview
        </button>
      </div>
    </div>
  );
}
