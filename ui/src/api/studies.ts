import { useQuery } from "@tanstack/react-query";
import { apiFetch } from "./client";
import type { Effector, Locus, LocusGene, Study, StudyDetail } from "./types";

export const fetchStudies = () => apiFetch<Study[]>("/studies");

export const fetchStudy = (id: string) =>
  apiFetch<StudyDetail>(`/studies/${encodeURIComponent(id)}`);

export const fetchStudyLoci = (id: string) =>
  apiFetch<Locus[]>(`/studies/${encodeURIComponent(id)}/loci`);

export const fetchStudyEffectors = (id: string) =>
  apiFetch<Effector[]>(`/studies/${encodeURIComponent(id)}/effectors`);

export const fetchAllLoci = (limit = 500) =>
  apiFetch<Locus[]>(`/loci?limit=${limit}`);

export const fetchLocusGenes = (locusId: string) =>
  apiFetch<LocusGene[]>(`/loci/${encodeURIComponent(locusId)}/genes`);

export const useStudies = () =>
  useQuery({ queryKey: ["studies"], queryFn: fetchStudies });

export const useStudy = (id: string) =>
  useQuery({
    queryKey: ["studies", id],
    queryFn: () => fetchStudy(id),
    enabled: !!id,
  });

export const useStudyLoci = (id: string) =>
  useQuery({
    queryKey: ["studies", id, "loci"],
    queryFn: () => fetchStudyLoci(id),
    enabled: !!id,
  });

export const useStudyEffectors = (id: string) =>
  useQuery({
    queryKey: ["studies", id, "effectors"],
    queryFn: () => fetchStudyEffectors(id),
    enabled: !!id,
  });

export const useAllLoci = () =>
  useQuery({ queryKey: ["loci"], queryFn: () => fetchAllLoci() });

export const useLocusGenes = (locusId: string) =>
  useQuery({
    queryKey: ["loci", locusId, "genes"],
    queryFn: () => fetchLocusGenes(locusId),
    enabled: !!locusId,
  });
