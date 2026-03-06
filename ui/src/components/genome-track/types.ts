/** Chromosome info from seqcol API */
export type ChromInfo = {
  name: string; // "chr1", "chr2", ..., "chrX"
  length: number; // bp
};

/** A locus positioned on the genome track */
export type TrackLocus = {
  id: string;
  chr: string;
  start: number;
  end: number;
  label: string; // locus_name or lead_rsid
  trait?: string; // for color coding on dashboard
  score?: number; // top gene score, for marker sizing
  pvalue?: number;
};

/** A cluster of nearby loci (rendered when zoomed out) */
export type LocusCluster = {
  type: "cluster";
  count: number;
  loci: TrackLocus[];
  chr: string;
  start: number;
  end: number;
  centerPixel: number;
};

/** Current viewport state in base-pair coordinates */
export type ViewState = {
  startBp: number; // absolute bp position (across all chromosomes)
  endBp: number;
};

/** Item that can be rendered on the track: either a single locus or a cluster */
export type TrackItem = TrackLocus | LocusCluster;

export function isCluster(item: TrackItem): item is LocusCluster {
  return "type" in item && item.type === "cluster";
}
