// --- Genes ---

export interface GeneSearchResult {
  gene: string;
  ensembl_gene_id: string;
  searchable_text?: string;
  score?: number;
  [key: string]: unknown;
}

export interface Gene {
  gene_symbol: string;
  ensembl_gene_id: string;
  gene_name: string;
  chromosome: string;
  start_position: number;
  end_position: number;
  strand: string;
  genome_build: string;
  [key: string]: unknown;
}

export interface GeneEvidence {
  gene_symbol: string;
  evidence_category: string;
  evidence_level: "locus" | "gene";
  source_tag: string;
  score: number | string;
  // Locus-level fields
  locus_id?: string;
  evidence_stream?: string;
  pvalue?: number | string;
  effect_size?: number | string;
  tissue?: string;
  cell_type?: string;
  is_supporting?: boolean | string;
  // Gene-level fields
  evidence_type?: string;
  trait?: string;
}

export interface GeneScore {
  locus_id: string;
  gene_symbol: string;
  distance_to_lead_kb: number | string;
  is_nearest_gene: boolean | string;
  is_within_locus: boolean | string;
  integration_method: string;
  integration_score: number | string;
  integration_rank: number | string;
  is_predicted_effector: boolean | string;
  locus_name: string;
  chromosome: string;
  start_position: number;
  end_position: number;
  study_id: string;
}

// --- Studies ---

export interface Study {
  study_id: string;
  trait: string;
  trait_description: string;
  trait_ontology_id: string;
  study_description: string;
  gwas_source: string;
  ancestry: string;
  sample_size: number | string;
  doi: string;
  year: number | string;
  n_loci: number | string;
}

export interface StudyDetail extends Study {
  n_loci_actual?: number;
  n_candidate_genes?: number;
  n_effectors?: number;
  evidence_categories?: string[];
  [key: string]: unknown;
}

export interface Locus {
  locus_id: string;
  locus_name: string;
  chromosome: string;
  start_position: number;
  end_position: number;
  lead_variant_id: string;
  lead_rsid: string;
  lead_pvalue: number | string;
  locus_source: string;
  n_signals: number | string;
  n_candidate_genes: number | string;
  study_id?: string;
  trait?: string;
  top_gene?: string;
  top_gene_score?: number | string;
}

export interface Effector {
  locus_id: string;
  locus_name: string;
  chromosome: string;
  start_position: number;
  end_position: number;
  gene_symbol: string;
  integration_score: number | string;
  integration_rank: number | string;
  is_predicted_effector: boolean | string;
}

// --- Locus evidence matrix ---

export interface LocusGeneEvidence {
  evidence_category: string;
  evidence_stream: string;
  source_tag: string;
  pvalue: number | string;
  effect_size: number | string;
  score: number | string;
  tissue: string;
  cell_type: string;
  is_supporting: boolean | string;
}

export interface LocusGene {
  gene_symbol: string;
  distance_to_lead_kb: number | string;
  is_nearest_gene: boolean | string;
  is_within_locus: boolean | string;
  integration_score: number | string;
  integration_rank: number | string;
  is_predicted_effector: boolean | string;
  evidence: LocusGeneEvidence[];
}

// --- Traits (client-side grouping) ---

export interface TraitGroup {
  trait: string;
  traitDescription: string;
  studies: Study[];
  totalLoci: number;
}

// --- Sources ---

export interface Source {
  name: string;
  source_type: string;
  url?: string;
  display_name?: string;
  description?: string;
  data_type?: string;
  [key: string]: unknown;
}

export interface SourceProvenance {
  source_tag: string;
  source_name: string;
  source_type: string;
  evidence_category: string;
  is_integrated: boolean | string;
  version: string;
  url: string;
  citation: string;
  date_imported: string;
  record_count: number | string;
}

export interface ImportRequest {
  name: string;
  data: Record<string, unknown>[];
  description?: string;
  display_name?: string;
  data_type?: string;
  source_type?: string;
  gene_column?: string;
  include_in_search?: boolean;
  url?: string;
  sheet?: string;
  skip_rows?: number;
}

export interface ImportResult {
  success: boolean;
  imported?: string;
  rows?: number;
  error?: string;
}

export interface MutationResult {
  success: boolean;
  error?: string;
  [key: string]: unknown;
}

// --- DB ---

export interface ChromSizes {
  names: string[];
  lengths: number[];
}

export interface DbStatus {
  n_studies: number;
  n_loci: number;
  n_genes: number;
  n_evidence_rows: number;
  n_sources: number;
  has_pegasus: boolean;
  genome_build: string;
  package_version: string;
}

export interface TableInfo {
  name: string;
  row_count: number;
  [key: string]: unknown;
}

export type EvidenceCategories = Record<string, string>;
