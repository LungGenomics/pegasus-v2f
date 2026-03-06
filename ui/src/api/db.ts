import { useQuery } from "@tanstack/react-query";
import { apiFetch, apiPost } from "./client";
import type { ChromSizes, DbStatus, EvidenceCategories, TableInfo } from "./types";

export const fetchStatus = () => apiFetch<DbStatus>("/db/status");

export const fetchTables = () => apiFetch<TableInfo[]>("/db/tables");

export const fetchConfig = () =>
  apiFetch<Record<string, unknown>>("/db/config");

export const fetchEvidenceCategories = () =>
  apiFetch<EvidenceCategories>("/db/evidence-categories");

export const executeQuery = (query: string) =>
  apiPost<Record<string, unknown>[]>("/db/query", { query });

export const useDbStatus = () =>
  useQuery({ queryKey: ["db", "status"], queryFn: fetchStatus });

export const useTables = () =>
  useQuery({ queryKey: ["db", "tables"], queryFn: fetchTables });

export const useDbConfig = () =>
  useQuery({ queryKey: ["db", "config"], queryFn: fetchConfig });

export const useEvidenceCategories = () =>
  useQuery({
    queryKey: ["db", "evidence-categories"],
    queryFn: fetchEvidenceCategories,
    staleTime: Infinity,
  });

export const fetchChromSizes = () =>
  apiFetch<ChromSizes>("/db/chrom-sizes");

export const useChromSizes = () =>
  useQuery({
    queryKey: ["db", "chrom-sizes"],
    queryFn: fetchChromSizes,
    staleTime: Infinity,
  });
