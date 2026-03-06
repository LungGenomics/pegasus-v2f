import { useMemo } from "react";
import { useQueries } from "@tanstack/react-query";
import { useStudies, fetchStudyLoci } from "./studies";
import type { Locus, TraitGroup } from "./types";

/** Groups studies by trait name. No new API calls — derives from useStudies(). */
export function useTraitGroups() {
  const studiesQ = useStudies();

  const data = useMemo(() => {
    if (!studiesQ.data) return undefined;
    const map = new Map<string, TraitGroup>();
    for (const s of studiesQ.data) {
      const existing = map.get(s.trait);
      if (existing) {
        existing.studies.push(s);
        existing.totalLoci += Number(s.n_loci) || 0;
      } else {
        map.set(s.trait, {
          trait: s.trait,
          traitDescription: s.trait_description,
          studies: [s],
          totalLoci: Number(s.n_loci) || 0,
        });
      }
    }
    return [...map.values()];
  }, [studiesQ.data]);

  return {
    data,
    isLoading: studiesQ.isLoading,
    error: studiesQ.error,
  };
}

/** Fetches loci for all studies matching a trait, merging results. */
export function useTraitLoci(trait: string) {
  const studiesQ = useStudies();

  const traitStudies = useMemo(
    () => (studiesQ.data ?? []).filter((s) => s.trait === trait),
    [studiesQ.data, trait],
  );

  const lociQueries = useQueries({
    queries: traitStudies.map((s) => ({
      queryKey: ["studies", s.study_id, "loci"] as const,
      queryFn: () => fetchStudyLoci(s.study_id),
      enabled: !!s.study_id,
    })),
  });

  const isLoading =
    studiesQ.isLoading || lociQueries.some((q) => q.isLoading);
  const error =
    studiesQ.error ?? lociQueries.find((q) => q.error)?.error ?? null;

  const data = useMemo(() => {
    if (isLoading) return undefined;
    const merged: Locus[] = [];
    for (let i = 0; i < traitStudies.length; i++) {
      const study = traitStudies[i]!;
      const loci = lociQueries[i]?.data;
      if (loci) {
        for (const l of loci) {
          merged.push({ ...l, study_id: study.study_id, trait: study.trait });
        }
      }
    }
    return merged;
  }, [isLoading, traitStudies, lociQueries]);

  return {
    data,
    isLoading,
    error,
    studies: traitStudies,
  };
}
