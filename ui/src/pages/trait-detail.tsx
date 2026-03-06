import { useState, useMemo, useCallback } from "react";
import { useParams, useSearchParams } from "react-router";
import { useTraitLoci } from "../api/traits";
import { useChromSizes } from "../api/db";
import { evidenceMatrixUrl, metadataUrl, pegListUrl } from "../api/exports";
import { PageHeader } from "../components/layout/page-header";
import { DataTable, type Column } from "../components/data-table";
import { DownloadButton } from "../components/download-button";
import { Loading, ErrorAlert } from "../components/loading";
import { GenomeTrack } from "../components/genome-track/genome-track";
import { LocusDetailPane } from "../components/locus-detail-pane/locus-detail-pane";
import { formatCoordinate, formatPvalue } from "../lib/format";
import type { TrackLocus, ViewState } from "../components/genome-track/types";
import type { Locus } from "../api/types";
import { buildChromList, chromOffsets, toAbsolute } from "../lib/genome-coords";

const STUDY_PALETTE = [
  "#6366f1", "#ec4899", "#14b8a6", "#f59e0b",
  "#8b5cf6", "#ef4444", "#06b6d4", "#84cc16",
];

export function TraitDetailPage() {
  const { trait: rawTrait } = useParams<{ trait: string }>();
  const trait = rawTrait ? decodeURIComponent(rawTrait) : "";
  const [searchParams, setSearchParams] = useSearchParams();

  const { data: loci, isLoading, error, studies } = useTraitLoci(trait);
  const chromQ = useChromSizes();

  const multiStudy = studies.length > 1;

  // Study color map (only matters for multi-study)
  const studyColors = useMemo(() => {
    const colors: Record<string, string> = {};
    studies.forEach((s, i) => {
      colors[s.study_id] = STUDY_PALETTE[i % STUDY_PALETTE.length]!;
    });
    return colors;
  }, [studies]);

  // Selected locus from URL params
  const selectedLocusId = searchParams.get("locus") ?? undefined;

  const setSelectedLocus = useCallback(
    (id: string | null) => {
      setSearchParams((prev) => {
        const next = new URLSearchParams(prev);
        if (id) {
          next.set("locus", id);
        } else {
          next.delete("locus");
        }
        return next;
      });
    },
    [setSearchParams],
  );

  // Map to TrackLocus — color by study when multi-study
  const trackLoci: TrackLocus[] = useMemo(() => {
    if (!loci) return [];
    return loci.map((l) => ({
      id: l.locus_id,
      chr: l.chromosome.startsWith("chr")
        ? l.chromosome
        : `chr${l.chromosome}`,
      start: l.start_position,
      end: l.end_position,
      label: l.locus_name || l.lead_rsid || l.locus_id,
      trait: multiStudy ? l.study_id : undefined,
      pvalue:
        typeof l.lead_pvalue === "number"
          ? l.lead_pvalue
          : parseFloat(String(l.lead_pvalue)) || undefined,
    }));
  }, [loci, multiStudy]);

  // Track genome track viewport
  const [trackView, setTrackView] = useState<ViewState | null>(null);

  // Filter loci to those visible in the current viewport
  const visibleLoci = useMemo(() => {
    if (!loci || !chromQ.data) return loci ?? [];
    if (!trackView) return loci;

    const chroms = buildChromList(chromQ.data.names, chromQ.data.lengths);
    const { offsets } = chromOffsets(chroms);

    const filtered = loci.filter((l) => {
      const chr = l.chromosome.startsWith("chr")
        ? l.chromosome
        : `chr${l.chromosome}`;
      try {
        const start = toAbsolute(chr, l.start_position, offsets);
        const end = toAbsolute(chr, l.end_position, offsets);
        return end >= trackView.startBp && start <= trackView.endBp;
      } catch {
        return false;
      }
    });

    return filtered.sort(
      (a, b) => Number(b.n_candidate_genes) - Number(a.n_candidate_genes),
    );
  }, [loci, chromQ.data, trackView]);

  const selectedLocus = loci?.find((l) => l.locus_id === selectedLocusId);

  // Loci table columns — add Source column for multi-study
  const lociColumns: Column<Locus & Record<string, unknown>>[] = useMemo(() => {
    const cols: Column<Locus & Record<string, unknown>>[] = [
      { key: "locus_name", header: "Locus" },
      {
        key: "region" as keyof Locus,
        header: "Region",
        render: (row) =>
          formatCoordinate(row.chromosome, row.start_position, row.end_position),
      },
      { key: "n_candidate_genes", header: "Candidates" },
      { key: "top_gene", header: "Top Gene" },
    ];
    if (multiStudy) {
      cols.splice(1, 0, {
        key: "study_id",
        header: "Source",
        render: (row) => {
          const color = studyColors[row.study_id ?? ""];
          const study = studies.find((s) => s.study_id === row.study_id);
          return (
            <span className="flex items-center gap-1.5">
              {color && (
                <span
                  className="w-2 h-2 rounded-full inline-block shrink-0"
                  style={{ backgroundColor: color }}
                />
              )}
              {study?.gwas_source ?? row.study_id}
            </span>
          );
        },
      });
    }
    return cols;
  }, [multiStudy, studyColors, studies]);

  if (isLoading) return <Loading />;
  if (error) return <ErrorAlert message={error.message} />;
  if (!studies.length) return null;

  // Use first study's detail for single-study metadata
  const primaryStudy = studies[0]!;

  return (
    <div>
      <PageHeader
        title={trait}
        description={primaryStudy.trait_description}
        breadcrumbs={[
          { label: "Traits", to: "/" },
          { label: trait },
        ]}
      />

      {/* Study metadata badges */}
      <div className="flex flex-wrap gap-3 mb-4 text-sm">
        {multiStudy ? (
          studies.map((s) => {
            const color = studyColors[s.study_id];
            return (
              <span key={s.study_id} className="badge badge-outline gap-1.5">
                {color && (
                  <span
                    className="w-2 h-2 rounded-full inline-block"
                    style={{ backgroundColor: color }}
                  />
                )}
                {s.gwas_source}
                {s.ancestry ? ` · ${s.ancestry}` : ""}
                {s.sample_size && s.sample_size !== "-"
                  ? ` · N=${s.sample_size}`
                  : ""}
              </span>
            );
          })
        ) : (
          <>
            {primaryStudy.gwas_source && (
              <span className="badge badge-outline">
                GWAS: {primaryStudy.gwas_source}
              </span>
            )}
            {primaryStudy.ancestry && (
              <span className="badge badge-outline">
                Ancestry: {primaryStudy.ancestry}
              </span>
            )}
            {primaryStudy.sample_size && (
              <span className="badge badge-outline">
                N = {primaryStudy.sample_size}
              </span>
            )}
          </>
        )}
      </div>

      {/* Export buttons — one set per study */}
      <div className="flex flex-wrap gap-2 mb-4">
        {studies.map((s) => (
          <div key={s.study_id} className="flex gap-2">
            {multiStudy && (
              <span className="text-xs self-center text-base-content/50">
                {s.gwas_source}:
              </span>
            )}
            <DownloadButton
              href={evidenceMatrixUrl(s.study_id)}
              label="Evidence Matrix"
            />
            <DownloadButton href={pegListUrl(s.study_id)} label="PEG List" />
            <DownloadButton
              href={metadataUrl(s.study_id)}
              label="Metadata YAML"
            />
          </div>
        ))}
      </div>

      {/* Genome track */}
      {chromQ.data && (
        <>
          <GenomeTrack
            loci={trackLoci}
            selectedLocusId={selectedLocusId}
            onLocusSelect={(id) => setSelectedLocus(id)}
            onViewChange={setTrackView}
            chromNames={chromQ.data.names}
            chromLengths={chromQ.data.lengths}
            traitColors={multiStudy ? studyColors : undefined}
            className="mb-2"
          />
          {multiStudy && (
            <div className="flex flex-wrap gap-3 mb-2">
              {studies.map((s) => {
                const color = studyColors[s.study_id];
                return (
                  <div
                    key={s.study_id}
                    className="flex items-center gap-1 text-xs"
                  >
                    {color && (
                      <span
                        className="w-3 h-3 rounded-sm inline-block"
                        style={{ backgroundColor: color }}
                      />
                    )}
                    {s.gwas_source}
                  </div>
                );
              })}
            </div>
          )}
        </>
      )}
      {chromQ.isLoading && (
        <div className="h-16 flex items-center justify-center text-base-content/40">
          Loading genome track...
        </div>
      )}

      {/* Detail pane (when a locus is selected) */}
      {selectedLocus && (
        <div className="my-4">
          <LocusDetailPane
            locus={selectedLocus}
            onClose={() => setSelectedLocus(null)}
          />
        </div>
      )}

      {/* Loci table */}
      <div className="mt-4">
        <h3 className="text-lg font-semibold mb-2">
          Loci ({visibleLoci.length}
          {loci && visibleLoci.length !== loci.length
            ? ` of ${loci.length}`
            : ""}
          )
        </h3>
        <DataTable
          data={visibleLoci as (Locus & Record<string, unknown>)[]}
          columns={lociColumns}
          onRowClick={(row) => setSelectedLocus(row.locus_id)}
          rowKey={(row) => row.locus_id}
          emptyMessage="No loci in view"
        />
      </div>
    </div>
  );
}
