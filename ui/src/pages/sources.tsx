import { useState } from "react";
import { RefreshCw, Trash2 } from "lucide-react";
import {
  useDeleteSource,
  useImportSource,
  useMaterialize,
  useSources,
  useUpdateSource,
  previewGoogleSheet,
} from "../api/sources";
import { PageHeader } from "../components/layout/page-header";
import { DataTable, type Column } from "../components/data-table";
import { DropZone } from "../components/drop-zone";
import { PreviewTable } from "../components/preview-table";
import { Loading, ErrorAlert } from "../components/loading";
import { parseCsv } from "../lib/csv-parse";
import { detectGeneColumn } from "../lib/gene-column-detect";
import type { Source } from "../api/types";

export function SourcesPage() {
  const { data: sources, isLoading, error } = useSources();
  const deleteMut = useDeleteSource();
  const updateMut = useUpdateSource();
  const materializeMut = useMaterialize();
  const importMut = useImportSource();

  // Import flow state
  const [preview, setPreview] = useState<Record<string, unknown>[]>([]);
  const [importError, setImportError] = useState("");
  const [sourceName, setSourceName] = useState("");
  const [description, setDescription] = useState("");
  const [category, setCategory] = useState("custom");
  const [geneColumn, setGeneColumn] = useState("");
  const [sourceUrl, setSourceUrl] = useState("");

  function handleFile(file: File) {
    setImportError("");
    const reader = new FileReader();
    reader.onload = () => {
      const rows = parseCsv(reader.result as string);
      if (rows.length === 0) {
        setImportError("Could not parse file — is it CSV or TSV?");
        return;
      }
      setPreview(rows);
      setSourceName(file.name.replace(/\.[^.]+$/, ""));
      const cols = Object.keys(rows[0]!);
      setGeneColumn(detectGeneColumn(cols) ?? cols[0] ?? "");
    };
    reader.readAsText(file);
  }

  async function handleUrl(url: string) {
    setImportError("");
    setSourceUrl(url);
    try {
      const rows = await previewGoogleSheet(url);
      if (rows.length === 0) {
        setImportError("Sheet returned no data");
        return;
      }
      setPreview(rows);
      const cols = Object.keys(rows[0]!);
      setGeneColumn(detectGeneColumn(cols) ?? cols[0] ?? "");
      setSourceName("google_sheet");
    } catch (e) {
      setImportError(e instanceof Error ? e.message : "Preview failed");
    }
  }

  async function handleImport() {
    setImportError("");
    const result = await importMut.mutateAsync({
      name: sourceName,
      data: preview as Record<string, unknown>[],
      description,
      data_type: category,
      gene_column: geneColumn,
      source_type: sourceUrl ? "googlesheets" : "file",
      url: sourceUrl,
    });
    if (!result.success) {
      setImportError(result.error ?? "Import failed");
      return;
    }
    // Reset
    setPreview([]);
    setSourceName("");
    setDescription("");
    setCategory("custom");
    setGeneColumn("");
    setSourceUrl("");
  }

  function resetImport() {
    setPreview([]);
    setImportError("");
    setSourceName("");
    setDescription("");
    setSourceUrl("");
  }

  const columns: Column<Source & Record<string, unknown>>[] = [
    { key: "name", header: "Name" },
    { key: "display_name", header: "Display Name" },
    { key: "source_type", header: "Type" },
    { key: "data_type", header: "Category" },
    {
      key: "_actions",
      header: "",
      sortable: false,
      render: (row) => (
        <div className="flex gap-1">
          <button
            className="btn btn-ghost btn-xs"
            title="Refresh"
            onClick={(e) => {
              e.stopPropagation();
              updateMut.mutate(row.name);
            }}
          >
            <RefreshCw className="size-3.5" />
          </button>
          <button
            className="btn btn-ghost btn-xs text-error"
            title="Delete"
            onClick={(e) => {
              e.stopPropagation();
              if (confirm(`Delete source "${row.name}"?`))
                deleteMut.mutate(row.name);
            }}
          >
            <Trash2 className="size-3.5" />
          </button>
        </div>
      ),
    },
  ];

  return (
    <div>
      <PageHeader
        title="Sources"
        actions={
          <button
            className="btn btn-sm btn-secondary"
            onClick={() => materializeMut.mutate()}
            disabled={materializeMut.isPending}
          >
            {materializeMut.isPending ? "Re-scoring..." : "Re-score All"}
          </button>
        }
      />

      {/* Import flow */}
      <div className="card bg-base-100 shadow-sm p-6 mb-6">
        <h2 className="font-semibold mb-3">Import Data</h2>

        {preview.length === 0 ? (
          <DropZone onFile={handleFile} onUrl={handleUrl} />
        ) : (
          <div>
            <div className="flex justify-between items-center mb-2">
              <h3 className="text-sm font-medium">Preview</h3>
              <button
                className="btn btn-ghost btn-xs"
                onClick={resetImport}
              >
                Clear
              </button>
            </div>

            <PreviewTable rows={preview} />

            <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-3 mt-4">
              <label className="form-control w-full">
                <div className="label">
                  <span className="label-text">Source name</span>
                </div>
                <input
                  type="text"
                  className="input input-bordered input-sm w-full"
                  value={sourceName}
                  onChange={(e) => setSourceName(e.target.value)}
                />
              </label>

              <label className="form-control w-full">
                <div className="label">
                  <span className="label-text">Description</span>
                </div>
                <input
                  type="text"
                  className="input input-bordered input-sm w-full"
                  value={description}
                  onChange={(e) => setDescription(e.target.value)}
                />
              </label>

              <label className="form-control w-full">
                <div className="label">
                  <span className="label-text">Gene column</span>
                </div>
                <select
                  className="select select-bordered select-sm w-full"
                  value={geneColumn}
                  onChange={(e) => setGeneColumn(e.target.value)}
                >
                  {Object.keys(preview[0]!).map((col) => (
                    <option key={col} value={col}>
                      {col}
                    </option>
                  ))}
                </select>
              </label>

              <label className="form-control w-full">
                <div className="label">
                  <span className="label-text">Category</span>
                </div>
                <input
                  type="text"
                  className="input input-bordered input-sm w-full"
                  value={category}
                  onChange={(e) => setCategory(e.target.value)}
                />
              </label>
            </div>

            <div className="mt-4">
              <button
                className="btn btn-primary btn-sm"
                onClick={handleImport}
                disabled={importMut.isPending || !sourceName}
              >
                {importMut.isPending ? "Importing..." : "Import"}
              </button>
            </div>
          </div>
        )}

        {importError && (
          <div role="alert" className="alert alert-error mt-3">
            <span>{importError}</span>
          </div>
        )}
      </div>

      {/* Source list */}
      {isLoading ? (
        <Loading />
      ) : error ? (
        <ErrorAlert message={error.message} />
      ) : (
        <DataTable
          data={(sources ?? []) as (Source & Record<string, unknown>)[]}
          columns={columns}
          rowKey={(row) => row.name}
          emptyMessage="No sources loaded"
        />
      )}
    </div>
  );
}
