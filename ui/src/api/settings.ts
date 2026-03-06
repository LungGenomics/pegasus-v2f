import { useMutation, useQueryClient } from "@tanstack/react-query";
import { apiPatch, apiPost } from "./client";
import type { MutationResult } from "./types";

export const updateMeta = (key: string, value: string) =>
  apiPatch<MutationResult>("/db/meta", { key, value });

export const reconnectDb = (db: string) =>
  apiPost<Record<string, unknown>>("/db/reconnect", { db });

export const useUpdateMeta = () => {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: ({ key, value }: { key: string; value: string }) =>
      updateMeta(key, value),
    onSuccess: () => {
      void qc.invalidateQueries({ queryKey: ["db"] });
    },
  });
};

export const useReconnectDb = () => {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: reconnectDb,
    onSuccess: () => {
      void qc.invalidateQueries();
    },
  });
};
