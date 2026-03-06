import { useQuery } from "@tanstack/react-query";
import { apiFetch } from "./client";
import type { Gene, GeneEvidence, GeneScore, GeneSearchResult } from "./types";

export interface PaginatedResponse<T> {
  results: T[];
  total: number;
}

const PAGE_SIZE = 50;

export const searchGenes = (search: string, limit = PAGE_SIZE, offset = 0) =>
  apiFetch<PaginatedResponse<GeneSearchResult>>(
    `/genes?search=${encodeURIComponent(search)}&limit=${limit}&offset=${offset}`,
  );

export const fetchGene = (gene: string) =>
  apiFetch<Gene>(`/genes/${encodeURIComponent(gene)}`);

export const fetchGeneEvidence = (gene: string) =>
  apiFetch<GeneEvidence[]>(`/genes/${encodeURIComponent(gene)}/evidence`);

export const fetchGeneScores = (gene: string) =>
  apiFetch<GeneScore[]>(`/genes/${encodeURIComponent(gene)}/scores`);

export const useGeneSearch = (search: string, offset = 0) =>
  useQuery({
    queryKey: ["genes", "search", search, offset],
    queryFn: () => searchGenes(search, PAGE_SIZE, offset),
  });

export const useGene = (gene: string) =>
  useQuery({
    queryKey: ["genes", gene],
    queryFn: () => fetchGene(gene),
    enabled: !!gene,
  });

export const useGeneEvidence = (gene: string) =>
  useQuery({
    queryKey: ["genes", gene, "evidence"],
    queryFn: () => fetchGeneEvidence(gene),
    enabled: !!gene,
  });

export const useGeneScores = (gene: string) =>
  useQuery({
    queryKey: ["genes", gene, "scores"],
    queryFn: () => fetchGeneScores(gene),
    enabled: !!gene,
  });
