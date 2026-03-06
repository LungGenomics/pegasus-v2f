import { X } from "lucide-react";
import { useLocusGenes } from "../../api/studies";
import { useEvidenceCategories } from "../../api/db";
import { Loading, ErrorAlert } from "../loading";
import { EvidenceHeatmap } from "./evidence-heatmap";
import { formatPvalue, formatCoordinate } from "../../lib/format";
import type { Locus } from "../../api/types";

type Props = {
  locus: Locus;
  onClose: () => void;
};

/**
 * Detail pane that appears below the genome track when a locus is selected.
 * Shows locus header info + evidence heatmap.
 */
export function LocusDetailPane({ locus, onClose }: Props) {
  const { data: genes, isLoading, error } = useLocusGenes(locus.locus_id);
  const { data: categories } = useEvidenceCategories();

  return (
    <div className="card bg-base-100 shadow-md border border-base-300 animate-in slide-in-from-top-2 duration-200">
      <div className="card-body p-4">
        {/* Header */}
        <div className="flex items-start justify-between gap-4">
          <div>
            <h3 className="text-lg font-semibold">
              {locus.locus_name || locus.locus_id}
            </h3>
            <p className="text-sm text-base-content/60">
              {formatCoordinate(
                locus.chromosome,
                locus.start_position,
                locus.end_position,
              )}
              {locus.lead_rsid && locus.lead_rsid !== "-" && (
                <span className="ml-3">Lead SNP: {locus.lead_rsid}</span>
              )}
              {locus.lead_pvalue && locus.lead_pvalue !== "-" && (
                <span className="ml-3">
                  P: {formatPvalue(locus.lead_pvalue)}
                </span>
              )}
              <span className="ml-3">
                {locus.n_candidate_genes} candidate gene
                {locus.n_candidate_genes !== 1 ? "s" : ""}
              </span>
            </p>
          </div>
          <button
            className="btn btn-ghost btn-sm btn-square"
            onClick={onClose}
            title="Close (Esc)"
          >
            <X size={16} />
          </button>
        </div>

        {/* Body */}
        {isLoading && <Loading />}
        {error && <ErrorAlert message={error.message} />}
        {genes && categories && (
          <EvidenceHeatmap genes={genes} categories={categories} />
        )}

        {/* Footer hint */}
        <p className="text-xs text-base-content/40 mt-2">
          Press Esc to close. Use arrow keys to navigate between loci.
        </p>
      </div>
    </div>
  );
}
