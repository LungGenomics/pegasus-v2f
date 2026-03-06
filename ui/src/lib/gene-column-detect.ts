const GENE_PATTERNS = [
  "gene",
  "gene_symbol",
  "gene_name",
  "genesymbol",
  "genename",
  "symbol",
  "hgnc_symbol",
  "hgnc",
];

export function detectGeneColumn(columns: string[]): string | null {
  const lower = columns.map((c) => c.toLowerCase());

  // Exact match first
  for (const pattern of GENE_PATTERNS) {
    const idx = lower.indexOf(pattern);
    if (idx !== -1) return columns[idx]!;
  }

  // Prefix match
  for (const pattern of GENE_PATTERNS) {
    const idx = lower.findIndex((c) => c.startsWith(pattern));
    if (idx !== -1) return columns[idx]!;
  }

  // Contains match
  for (const pattern of GENE_PATTERNS) {
    const idx = lower.findIndex((c) => c.includes(pattern));
    if (idx !== -1) return columns[idx]!;
  }

  return columns[0] ?? null;
}
