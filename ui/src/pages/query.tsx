import { useState } from "react";
import { Play, Download } from "lucide-react";
import { executeQuery } from "../api/db";
import { PageHeader } from "../components/layout/page-header";
import { DataTable, type Column } from "../components/data-table";
import { ErrorAlert } from "../components/loading";

export function QueryPage() {
  const [sql, setSql] = useState("");
  const [results, setResults] = useState<Record<string, unknown>[] | null>(
    null,
  );
  const [error, setError] = useState("");
  const [running, setRunning] = useState(false);

  async function run() {
    if (!sql.trim()) return;
    setError("");
    setRunning(true);
    try {
      const rows = await executeQuery(sql);
      setResults(rows);
    } catch (e) {
      setError(e instanceof Error ? e.message : "Query failed");
      setResults(null);
    } finally {
      setRunning(false);
    }
  }

  function downloadTsv() {
    if (!results || results.length === 0) return;
    const cols = Object.keys(results[0]!);
    const header = cols.join("\t");
    const body = results
      .map((row) => cols.map((c) => String(row[c] ?? "")).join("\t"))
      .join("\n");
    const blob = new Blob([`${header}\n${body}`], { type: "text/tab-separated-values" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = "query_results.tsv";
    a.click();
    URL.revokeObjectURL(url);
  }

  const columns: Column<Record<string, unknown>>[] = results?.length
    ? Object.keys(results[0]!).map((key) => ({
        key,
        header: key,
      }))
    : [];

  return (
    <div>
      <PageHeader
        title="Query"
        description="Run raw SQL against the database"
      />

      <div className="mb-4">
        <textarea
          className="textarea textarea-bordered w-full font-mono text-sm"
          rows={6}
          placeholder="SELECT * FROM studies LIMIT 10"
          value={sql}
          onChange={(e) => setSql(e.target.value)}
          onKeyDown={(e) => {
            if ((e.ctrlKey || e.metaKey) && e.key === "Enter") {
              e.preventDefault();
              run();
            }
          }}
        />
        <div className="flex gap-2 mt-2">
          <button
            className="btn btn-primary btn-sm gap-2"
            onClick={run}
            disabled={running || !sql.trim()}
          >
            <Play className="size-4" />
            {running ? "Running..." : "Run"}
          </button>
          <span className="text-xs text-base-content/50 self-center">
            Ctrl+Enter to run
          </span>
          {results && results.length > 0 && (
            <button
              className="btn btn-outline btn-sm gap-2 ml-auto"
              onClick={downloadTsv}
            >
              <Download className="size-4" />
              Download TSV
            </button>
          )}
        </div>
      </div>

      {error && <ErrorAlert message={error} />}

      {results && (
        <>
          <p className="text-sm text-base-content/60 mb-2">
            {results.length} row{results.length !== 1 && "s"}
          </p>
          <DataTable data={results} columns={columns} emptyMessage="No rows returned" />
        </>
      )}
    </div>
  );
}
