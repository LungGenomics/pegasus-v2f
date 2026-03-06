import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { apiDelete, apiFetch, apiPost } from "./client";
import type {
  ImportRequest,
  ImportResult,
  MutationResult,
  Source,
  SourceProvenance,
} from "./types";

export const fetchSources = () => apiFetch<Source[]>("/sources");

export const fetchProvenance = () =>
  apiFetch<SourceProvenance[]>("/sources/provenance");

export const previewGoogleSheet = (ss: string, sheet = "", skip = 0) =>
  apiPost<Record<string, unknown>[]>("/sources/preview", { ss, sheet, skip });

export const importSource = (req: ImportRequest) =>
  apiPost<ImportResult>("/sources/import", req);

export const updateSource = (name: string) =>
  apiPost<MutationResult>(`/sources/${encodeURIComponent(name)}/update`, {});

export const deleteSource = (name: string) =>
  apiDelete<MutationResult>(`/sources/${encodeURIComponent(name)}`);

export const materializeScores = () =>
  apiPost<MutationResult>("/sources/materialize", {});

export const useSources = () =>
  useQuery({ queryKey: ["sources"], queryFn: fetchSources });

export const useProvenance = () =>
  useQuery({ queryKey: ["sources", "provenance"], queryFn: fetchProvenance });

export const useImportSource = () => {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: importSource,
    onSuccess: () => {
      void qc.invalidateQueries({ queryKey: ["sources"] });
      void qc.invalidateQueries({ queryKey: ["db", "status"] });
    },
  });
};

export const useDeleteSource = () => {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: deleteSource,
    onSuccess: () => {
      void qc.invalidateQueries({ queryKey: ["sources"] });
      void qc.invalidateQueries({ queryKey: ["db", "status"] });
    },
  });
};

export const useUpdateSource = () => {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: updateSource,
    onSuccess: () => {
      void qc.invalidateQueries({ queryKey: ["sources"] });
    },
  });
};

export const useMaterialize = () => {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: materializeScores,
    onSuccess: () => {
      void qc.invalidateQueries({ queryKey: ["studies"] });
      void qc.invalidateQueries({ queryKey: ["loci"] });
      void qc.invalidateQueries({ queryKey: ["genes"] });
    },
  });
};
