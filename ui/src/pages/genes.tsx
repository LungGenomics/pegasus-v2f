import { useState } from "react";
import { Link, useSearchParams } from "react-router";
import { useGeneSearch } from "../api/genes";
import { SearchInput } from "../components/search-input";
import { DataTable, type Column } from "../components/data-table";
import { PageHeader } from "../components/layout/page-header";
import { Loading, ErrorAlert } from "../components/loading";
import type { GeneSearchResult } from "../api/types";

const PAGE_SIZE = 50;

const columns: Column<GeneSearchResult & Record<string, unknown>>[] = [
  {
    key: "gene",
    header: "Gene",
    render: (row) => (
      <Link
        to={`/genes/${encodeURIComponent(row.gene)}`}
        className="link link-primary font-mono font-semibold"
      >
        {row.gene}
      </Link>
    ),
  },
  { key: "ensembl_gene_id", header: "Ensembl ID" },
];

export function GenesPage() {
  const [params, setParams] = useSearchParams();
  const initialQ = params.get("q") ?? "";
  const page = Math.max(1, Number(params.get("page")) || 1);
  const [search, setSearch] = useState(initialQ);
  const offset = (page - 1) * PAGE_SIZE;
  const { data, isLoading, error } = useGeneSearch(search, offset);

  function handleSearch(v: string) {
    setSearch(v);
    setParams(v ? { q: v } : {}, { replace: true });
  }

  function setPage(p: number) {
    const next: Record<string, string> = {};
    if (search) next.q = search;
    if (p > 1) next.page = String(p);
    setParams(next, { replace: true });
  }

  const totalPages = data ? Math.ceil(data.total / PAGE_SIZE) : 0;
  const rangeStart = data && data.total > 0 ? offset + 1 : 0;
  const rangeEnd = data ? Math.min(offset + PAGE_SIZE, data.total) : 0;

  return (
    <div>
      <PageHeader title="Genes" description="Browse and search genes" />

      <div className="max-w-lg mb-6">
        <SearchInput
          value={search}
          onChange={handleSearch}
          placeholder="Filter by gene symbol or name..."
        />
      </div>

      {isLoading && <Loading />}
      {error && <ErrorAlert message={error.message} />}
      {data && (
        <>
          <p className="text-sm text-base-content/60 mb-2">
            {data.total} gene{data.total !== 1 && "s"}
            {search && " matching"}
            {data.total > 0 &&
              ` \u00b7 Showing ${rangeStart}\u2013${rangeEnd}`}
          </p>
          <DataTable
            data={
              data.results as (GeneSearchResult & Record<string, unknown>)[]
            }
            columns={columns}
            emptyMessage="No genes found"
          />
          {totalPages > 1 && (
            <div className="flex items-center justify-between mt-4">
              <button
                className="btn btn-sm"
                disabled={page <= 1}
                onClick={() => setPage(page - 1)}
              >
                Previous
              </button>
              <span className="text-sm text-base-content/60">
                Page {page} of {totalPages}
              </span>
              <button
                className="btn btn-sm"
                disabled={page >= totalPages}
                onClick={() => setPage(page + 1)}
              >
                Next
              </button>
            </div>
          )}
        </>
      )}
    </div>
  );
}
