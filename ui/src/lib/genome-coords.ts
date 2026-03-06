import type {
  ChromInfo,
  TrackLocus,
  LocusCluster,
  TrackItem,
} from "../components/genome-track/types";

/**
 * Build an ordered list of ChromInfo from parallel name/length arrays.
 */
export function buildChromList(
  names: string[],
  lengths: number[],
): ChromInfo[] {
  return names.map((name, i) => ({ name, length: lengths[i]! }));
}

/**
 * Compute cumulative offsets for each chromosome.
 * Returns a map of chr name -> absolute bp offset (start of that chromosome
 * in the linear genome layout). Also returns total genome length.
 *
 * Gap between chromosomes is configurable (in bp). Default ~2Mbp gives
 * a visible gap when zoomed to chromosome level.
 */
export function chromOffsets(
  chroms: ChromInfo[],
  gapBp = 50_000,
): { offsets: Map<string, number>; totalLength: number } {
  const offsets = new Map<string, number>();
  let pos = 0;
  for (const chr of chroms) {
    offsets.set(chr.name, pos);
    pos += chr.length + gapBp;
  }
  // Remove trailing gap
  if (chroms.length > 0) {
    pos -= gapBp;
  }
  return { offsets, totalLength: pos };
}

/**
 * Convert a genomic position (chr + bp) to an absolute bp coordinate.
 */
export function toAbsolute(
  chr: string,
  pos: number,
  offsets: Map<string, number>,
): number {
  const offset = offsets.get(chr);
  if (offset === undefined) {
    throw new Error(`Unknown chromosome: ${chr}`);
  }
  return offset + pos;
}

/**
 * Convert an absolute bp coordinate back to chr + position.
 */
export function fromAbsolute(
  absBp: number,
  chroms: ChromInfo[],
  offsets: Map<string, number>,
): { chr: string; pos: number } {
  for (let i = chroms.length - 1; i >= 0; i--) {
    const chr = chroms[i]!;
    const offset = offsets.get(chr.name)!;
    if (absBp >= offset) {
      return { chr: chr.name, pos: absBp - offset };
    }
  }
  return { chr: chroms[0]!.name, pos: 0 };
}

/**
 * Get absolute midpoint for a locus.
 */
export function locusMidpoint(
  locus: TrackLocus,
  offsets: Map<string, number>,
): number {
  const start = toAbsolute(locus.chr, locus.start, offsets);
  const end = toAbsolute(locus.chr, locus.end, offsets);
  return (start + end) / 2;
}

/**
 * Cluster loci that are closer than `minPixelGap` pixels apart.
 *
 * Expects loci sorted by absolute position. Uses a greedy sweep:
 * walk left-to-right, merge any locus whose pixel position is within
 * minPixelGap of the current cluster's rightmost pixel.
 */
export function clusterLoci(
  loci: TrackLocus[],
  bpToPixel: (chr: string, pos: number) => number,
  minPixelGap: number,
): TrackItem[] {
  if (loci.length === 0) return [];

  // Sort by pixel position of midpoint
  const withPixel = loci.map((l) => ({
    locus: l,
    px: bpToPixel(l.chr, (l.start + l.end) / 2),
  }));
  withPixel.sort((a, b) => a.px - b.px);

  const result: TrackItem[] = [];
  let currentGroup: typeof withPixel = [withPixel[0]!];

  for (let i = 1; i < withPixel.length; i++) {
    const last = currentGroup[currentGroup.length - 1]!;
    if (withPixel[i]!.px - last.px < minPixelGap) {
      currentGroup.push(withPixel[i]!);
    } else {
      result.push(finalizeGroup(currentGroup));
      currentGroup = [withPixel[i]!];
    }
  }
  result.push(finalizeGroup(currentGroup));

  return result;
}

function finalizeGroup(
  group: { locus: TrackLocus; px: number }[],
): TrackItem {
  if (group.length === 1) {
    return group[0]!.locus;
  }

  const loci = group.map((g) => g.locus);
  const pixels = group.map((g) => g.px);

  return {
    type: "cluster",
    count: loci.length,
    loci,
    chr: loci[0]!.chr, // cluster may span chroms, use first
    start: Math.min(...loci.map((l) => l.start)),
    end: Math.max(...loci.map((l) => l.end)),
    centerPixel: (Math.min(...pixels) + Math.max(...pixels)) / 2,
  } satisfies LocusCluster;
}

/**
 * Sort loci by absolute position on the genome.
 */
export function sortLociByPosition(
  loci: TrackLocus[],
  offsets: Map<string, number>,
): TrackLocus[] {
  return [...loci].sort(
    (a, b) => locusMidpoint(a, offsets) - locusMidpoint(b, offsets),
  );
}
