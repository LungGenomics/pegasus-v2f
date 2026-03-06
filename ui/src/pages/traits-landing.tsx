import { useState, useMemo, useCallback } from "react";
import { useNavigate, Link } from "react-router";
import { useDbStatus, useChromSizes } from "../api/db";
import { useAllLoci, useStudies } from "../api/studies";
import { useGeneSearch } from "../api/genes";
import { useTraitGroups } from "../api/traits";
import { SearchInput } from "../components/search-input";
import { Loading, ErrorAlert } from "../components/loading";
import { GenomeTrack } from "../components/genome-track/genome-track";
import { formatNumber } from "../lib/format";
import type { TrackLocus } from "../components/genome-track/types";

const TRAIT_PALETTE = [
  "#6366f1", "#ec4899", "#14b8a6", "#f59e0b",
  "#8b5cf6", "#ef4444", "#06b6d4", "#84cc16",
  "#f97316", "#3b82f6", "#10b981", "#e879f9",
];

export function TraitsLandingPage() {
  const navigate = useNavigate();
  const { data: status } = useDbStatus();
  const chromQ = useChromSizes();
  const lociQ = useAllLoci();
  const studiesQ = useStudies();
  const traitGroupsQ = useTraitGroups();
  const [search, setSearch] = useState("");
  const geneSearch = useGeneSearch(search);

  // Build trait color map
  const traitColors = useMemo(() => {
    if (!studiesQ.data) return {};
    const colors: Record<string, string> = {};
    const traits = [...new Set(studiesQ.data.map((s) => s.trait))];
    traits.forEach((t, i) => {
      colors[t] = TRAIT_PALETTE[i % TRAIT_PALETTE.length]!;
    });
    return colors;
  }, [studiesQ.data]);

  // Map loci to TrackLocus with trait for color coding
  const trackLoci: TrackLocus[] = useMemo(() => {
    if (!lociQ.data) return [];
    return lociQ.data.map((l) => ({
      id: l.locus_id,
      chr: l.chromosome.startsWith("chr")
        ? l.chromosome
        : `chr${l.chromosome}`,
      start: l.start_position,
      end: l.end_position,
      label: l.locus_name || l.lead_rsid || l.locus_id,
      trait: l.trait,
      pvalue:
        typeof l.lead_pvalue === "number"
          ? l.lead_pvalue
          : parseFloat(String(l.lead_pvalue)) || undefined,
    }));
  }, [lociQ.data]);

  const handleLocusSelect = useCallback(
    (id: string) => {
      const locus = lociQ.data?.find((l) => l.locus_id === id);
      if (locus?.trait) {
        navigate(`/traits/${encodeURIComponent(locus.trait)}?locus=${id}`);
      }
    },
    [lociQ.data, navigate],
  );

  if (traitGroupsQ.isLoading) return <Loading />;
  if (traitGroupsQ.error)
    return <ErrorAlert message={traitGroupsQ.error.message} />;

  return (
    <div>
      {/* Gene search */}
      <div className="max-w-xl mb-6">
        <SearchInput
          value={search}
          onChange={setSearch}
          placeholder="Search by gene symbol or name..."
        />
        {geneSearch.data && geneSearch.data.results.length > 0 && (
          <ul className="menu mt-2 bg-base-200 rounded-box max-h-60 overflow-y-auto">
            {geneSearch.data.results.slice(0, 10).map((g) => (
              <li key={g.ensembl_gene_id}>
                <Link to={`/genes/${encodeURIComponent(g.gene)}`}>
                  <span className="font-mono font-semibold">{g.gene}</span>
                  {g.ensembl_gene_id && (
                    <span className="text-xs opacity-60">
                      {g.ensembl_gene_id}
                    </span>
                  )}
                </Link>
              </li>
            ))}
          </ul>
        )}
      </div>

      {/* Genome track — all loci, color-coded by trait */}
      {chromQ.data && (
        <div className="mb-6">
          <GenomeTrack
            loci={trackLoci}
            onLocusSelect={handleLocusSelect}
            chromNames={chromQ.data.names}
            chromLengths={chromQ.data.lengths}
            traitColors={traitColors}
          />
          {Object.entries(traitColors).length > 0 && (
            <div className="flex flex-wrap gap-3 mt-2">
              {Object.entries(traitColors).map(([trait, color]) => (
                <div key={trait} className="flex items-center gap-1 text-xs">
                  <span
                    className="w-3 h-3 rounded-sm inline-block"
                    style={{ backgroundColor: color }}
                  />
                  {trait}
                </div>
              ))}
            </div>
          )}
        </div>
      )}
      {chromQ.isLoading && (
        <div className="h-16 flex items-center justify-center text-base-content/40 mb-6">
          Loading genome track...
        </div>
      )}

      {/* Trait cards */}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4 mb-6">
        {traitGroupsQ.data?.map((group) => (
          <Link
            key={group.trait}
            to={`/traits/${encodeURIComponent(group.trait)}`}
            className="card bg-base-100 shadow-sm hover:shadow-md transition-shadow"
          >
            <div className="card-body p-5">
              <div className="flex items-start gap-2">
                {traitColors[group.trait] && (
                  <span
                    className="w-3 h-3 rounded-sm mt-1 shrink-0"
                    style={{ backgroundColor: traitColors[group.trait] }}
                  />
                )}
                <div className="min-w-0">
                  <h2 className="card-title text-lg">{group.trait}</h2>
                  {group.traitDescription && (
                    <p className="text-sm text-base-content/60 mt-0.5">
                      {group.traitDescription}
                    </p>
                  )}
                </div>
              </div>

              {/* Study badges */}
              <div className="flex flex-wrap gap-1.5 mt-3">
                {group.studies.map((s) => (
                  <span
                    key={s.study_id}
                    className="badge badge-outline badge-sm"
                  >
                    {s.gwas_source}
                    {s.ancestry ? ` · ${s.ancestry}` : ""}
                    {s.sample_size && s.sample_size !== "-"
                      ? ` · N=${formatNumber(s.sample_size)}`
                      : ""}
                  </span>
                ))}
              </div>

              {/* Stats */}
              <div className="flex gap-4 mt-3 text-sm text-base-content/70">
                <span>{group.totalLoci} loci</span>
              </div>
            </div>
          </Link>
        ))}
      </div>

      {/* Footer stats */}
      {status && (
        <div className="text-xs text-base-content/40">
          {status.genome_build !== "-" && (
            <span>Genome: {status.genome_build}</span>
          )}
          {status.package_version !== "-" && (
            <span className="ml-4">v{status.package_version}</span>
          )}
        </div>
      )}
    </div>
  );
}
