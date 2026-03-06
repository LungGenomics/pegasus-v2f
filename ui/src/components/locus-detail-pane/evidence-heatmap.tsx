import { Link } from "react-router";
import type { LocusGene, LocusGeneEvidence } from "../../api/types";

type Props = {
  genes: LocusGene[];
  categories: Record<string, string>; // abbreviation -> full name
  onGeneClick?: (gene: string) => void;
};

/** Map category abbreviation to a hue for the heatmap cell fill */
const CATEGORY_HUES: Record<string, string> = {
  QTL: "217",   // blue
  COLOC: "217",
  GWAS: "271",  // purple
  PROX: "160",  // teal
  CODE: "0",    // red
  RARE: "0",
  EXP: "199",   // cyan
  EPIG: "199",
  CHROM: "199",
  REG: "38",    // amber
  FUNC: "142",  // green
  MOD: "142",
  DRUG: "38",
  PATH: "160",
  PPI: "160",
  KNOW: "220",  // neutral blue
  LIT: "220",
  CLIN: "0",
  OMICS: "217",
  PERT: "142",
  EVOL: "220",
  OTHER: "0",
};

/**
 * Gene × modality evidence grid.
 *
 * Rows = candidate genes sorted by integration_score descending.
 * Columns = all 22 PEGASUS evidence categories.
 * Cell color intensity = max score across evidence items in that category.
 * Empty cells show a dotted border to indicate missing data.
 */
export function EvidenceHeatmap({ genes, categories, onGeneClick }: Props) {
  const categoryKeys = Object.keys(categories);

  // Build evidence lookup: gene -> category -> evidence items
  const evidenceMap = new Map<
    string,
    Map<string, LocusGeneEvidence[]>
  >();
  for (const gene of genes) {
    const catMap = new Map<string, LocusGeneEvidence[]>();
    for (const ev of gene.evidence) {
      const cat = ev.evidence_category;
      if (!catMap.has(cat)) catMap.set(cat, []);
      catMap.get(cat)!.push(ev);
    }
    evidenceMap.set(gene.gene_symbol, catMap);
  }

  // Sort genes by score descending
  const sorted = [...genes].sort((a, b) => {
    const sa = typeof a.integration_score === "number" ? a.integration_score : parseFloat(String(a.integration_score)) || 0;
    const sb = typeof b.integration_score === "number" ? b.integration_score : parseFloat(String(b.integration_score)) || 0;
    return sb - sa;
  });

  return (
    <div className="overflow-x-auto">
      <table className="table table-xs">
        <thead>
          <tr>
            <th className="sticky left-0 bg-base-100 z-10">Gene</th>
            <th className="text-right">Score</th>
            <th className="text-center">PEG</th>
            {categoryKeys.map((cat) => (
              <th
                key={cat}
                className="text-center px-0.5"
                title={categories[cat]}
              >
                <span className="[writing-mode:vertical-lr] text-[10px] rotate-180">
                  {cat}
                </span>
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {sorted.map((gene) => {
            const catMap = evidenceMap.get(gene.gene_symbol);
            const score =
              typeof gene.integration_score === "number"
                ? gene.integration_score
                : parseFloat(String(gene.integration_score)) || 0;
            const isPeg =
              gene.is_predicted_effector === true ||
              gene.is_predicted_effector === "true";

            return (
              <tr key={gene.gene_symbol} className="hover">
                <td className="sticky left-0 bg-base-100 z-10 font-medium">
                  <Link
                    to={`/genes/${encodeURIComponent(gene.gene_symbol)}`}
                    className="link link-primary"
                    onClick={(e) => {
                      if (onGeneClick) {
                        e.preventDefault();
                        onGeneClick(gene.gene_symbol);
                      }
                    }}
                  >
                    {gene.gene_symbol}
                  </Link>
                </td>
                <td className="text-right font-mono text-xs tabular-nums">
                  {score.toFixed(2)}
                </td>
                <td className="text-center">
                  {isPeg && (
                    <span className="badge badge-xs badge-primary">PEG</span>
                  )}
                </td>
                {categoryKeys.map((cat) => {
                  const items = catMap?.get(cat);
                  if (!items || items.length === 0) {
                    return (
                      <td
                        key={cat}
                        className="px-0.5"
                      >
                        <div className="w-5 h-5 border border-dashed border-base-300 rounded-sm" />
                      </td>
                    );
                  }

                  // Max score in this category
                  const maxScore = Math.max(
                    ...items.map((e) =>
                      typeof e.score === "number"
                        ? e.score
                        : parseFloat(String(e.score)) || 0,
                    ),
                  );
                  const hue = CATEGORY_HUES[cat] ?? "0";
                  // Map score 0-1 to opacity 0.2-0.9
                  const opacity = 0.2 + Math.min(maxScore, 1) * 0.7;

                  return (
                    <td key={cat} className="px-0.5">
                      <div
                        className="w-5 h-5 rounded-sm cursor-pointer hover:ring-2 hover:ring-primary/50"
                        style={{
                          backgroundColor: `hsla(${hue}, 70%, 50%, ${opacity})`,
                        }}
                        title={`${categories[cat]}: ${items.length} item(s), max score ${maxScore.toFixed(2)}`}
                      />
                    </td>
                  );
                })}
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}
