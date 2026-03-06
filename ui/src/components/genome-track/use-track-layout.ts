import { useMemo } from "react";
import type { ChromInfo, ViewState } from "./types";
import {
  buildChromList,
  chromOffsets,
  toAbsolute,
  fromAbsolute,
} from "../../lib/genome-coords";

export type TrackLayout = {
  /** Convert genomic coordinates to pixel X position */
  bpToPixel: (chr: string, pos: number) => number;
  /** Convert pixel X position to genomic coordinates */
  pixelToBp: (px: number) => { chr: string; pos: number };
  /** Chromosomes visible in the current view */
  visibleChroms: { chr: string; startPx: number; widthPx: number }[];
  /** Ordered chromosome info */
  chroms: ChromInfo[];
  /** Total genome length in bp (including gaps) */
  totalLength: number;
  /** Chromosome offsets map */
  offsets: Map<string, number>;
  /** Basepairs per pixel at current zoom */
  bpPerPixel: number;
};

/**
 * Compute pixel positions from genomic coordinates for the current view.
 *
 * @param containerWidth - Width of the SVG container in pixels
 * @param view - Current viewport in absolute bp coordinates
 * @param names - Chromosome names from seqcol
 * @param lengths - Chromosome lengths from seqcol
 */
export function useTrackLayout(
  containerWidth: number,
  view: ViewState,
  names: string[],
  lengths: number[],
): TrackLayout {
  const chroms = useMemo(() => buildChromList(names, lengths), [names, lengths]);

  const { offsets, totalLength } = useMemo(
    () => chromOffsets(chroms),
    [chroms],
  );

  return useMemo(() => {
    const viewSpan = view.endBp - view.startBp;
    const bpPerPixel = viewSpan / containerWidth;
    const pxPerBp = containerWidth / viewSpan;

    const bpToPixel = (chr: string, pos: number): number => {
      const absBp = toAbsolute(chr, pos, offsets);
      return (absBp - view.startBp) * pxPerBp;
    };

    const pixelToBp = (px: number): { chr: string; pos: number } => {
      const absBp = view.startBp + px / pxPerBp;
      return fromAbsolute(absBp, chroms, offsets);
    };

    // Determine which chromosomes are visible
    const visibleChroms: { chr: string; startPx: number; widthPx: number }[] =
      [];
    for (const chr of chroms) {
      const chrStart = offsets.get(chr.name)!;
      const chrEnd = chrStart + chr.length;

      // Skip if well outside viewport (generous buffer so partially
      // visible chroms don't pop in/out at the edges)
      const buffer = viewSpan * 0.5;
      if (chrEnd < view.startBp - buffer || chrStart > view.endBp + buffer)
        continue;

      const startPx = (chrStart - view.startBp) * pxPerBp;
      const endPx = (chrEnd - view.startBp) * pxPerBp;
      visibleChroms.push({
        chr: chr.name,
        startPx,
        widthPx: endPx - startPx,
      });
    }

    return {
      bpToPixel,
      pixelToBp,
      visibleChroms,
      chroms,
      totalLength,
      offsets,
      bpPerPixel,
    };
  }, [containerWidth, view, chroms, offsets, totalLength]);
}
