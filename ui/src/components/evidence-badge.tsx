const CATEGORY_COLORS: Record<string, string> = {
  QTL: "badge-primary",
  COLOC: "badge-primary",
  GWAS: "badge-secondary",
  PROX: "badge-accent",
  CODE: "badge-error",
  RARE: "badge-error",
  EXP: "badge-info",
  EPIG: "badge-info",
  CHROM: "badge-info",
  REG: "badge-warning",
  FUNC: "badge-success",
  MOD: "badge-success",
  DRUG: "badge-warning",
  PATH: "badge-accent",
  PPI: "badge-accent",
  KNOW: "badge-neutral",
  LIT: "badge-neutral",
  CLIN: "badge-error",
  OMICS: "badge-primary",
  PERT: "badge-success",
  EVOL: "badge-neutral",
  OTHER: "badge-ghost",
};

export function EvidenceBadge({ category }: { category: string }) {
  const color = CATEGORY_COLORS[category] ?? "badge-ghost";
  return <span className={`badge badge-sm ${color}`}>{category}</span>;
}
