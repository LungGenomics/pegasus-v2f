// Exports use anchor-based downloads — no React Query needed.
// These helpers just build the URLs.

const API_BASE = "/api";

export function evidenceMatrixUrl(studyId: string, format: "tsv" | "json" = "tsv") {
  return `${API_BASE}/export/${encodeURIComponent(studyId)}/evidence-matrix?format=${format}`;
}

export function pegListUrl(studyId: string, format: "tsv" | "json" = "tsv") {
  return `${API_BASE}/export/${encodeURIComponent(studyId)}/peg-list?format=${format}`;
}

export function metadataUrl(studyId: string, format: "yaml" | "json" = "yaml") {
  return `${API_BASE}/export/${encodeURIComponent(studyId)}/metadata?format=${format}`;
}
