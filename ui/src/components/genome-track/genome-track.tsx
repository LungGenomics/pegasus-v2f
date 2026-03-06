import { useRef, useState, useCallback, useEffect, useMemo } from "react";
import type { TrackLocus, TrackItem, ViewState } from "./types";
import { isCluster } from "./types";
import { useTrackLayout } from "./use-track-layout";
import { useGenomeZoom } from "./use-genome-zoom";
import { ChromosomeTrack } from "./chromosome-track";
import { LocusMarkers } from "./locus-markers";
import {
  clusterLoci,
  sortLociByPosition,
  chromOffsets,
  buildChromList,
  toAbsolute,
} from "../../lib/genome-coords";
import { TrackControls } from "./track-controls";

// Layout constants
const LABEL_AREA = 16;
const MARKER_AREA = 6;
const GAP = 2; // space between triangles and bar
const BAR_Y = LABEL_AREA + MARKER_AREA + GAP; // 24
const BAR_HEIGHT = 4;
const CHR_LABEL_AREA = 12; // space for chr labels below bar
const TOTAL_HEIGHT = BAR_Y + BAR_HEIGHT + CHR_LABEL_AREA; // 40
const MIN_PIXEL_GAP = 8;

export type GenomeTrackProps = {
  loci: TrackLocus[];
  selectedLocusId?: string;
  onLocusSelect: (id: string) => void;
  onViewChange?: (view: ViewState) => void;
  chromNames: string[];
  chromLengths: number[];
  traitColors?: Record<string, string>;
  className?: string;
};

export function GenomeTrack({
  loci,
  selectedLocusId,
  onLocusSelect,
  onViewChange: onViewChangeProp,
  chromNames,
  chromLengths,
  traitColors,
  className,
}: GenomeTrackProps) {
  const containerRef = useRef<HTMLDivElement>(null);
  const svgRef = useRef<SVGSVGElement>(null);
  const [containerWidth, setContainerWidth] = useState(800);
  const [view, setView] = useState<ViewState>({ startBp: 0, endBp: 1 });

  // Observe container width
  useEffect(() => {
    const el = containerRef.current;
    if (!el) return;
    const obs = new ResizeObserver((entries) => {
      const w = entries[0]?.contentRect.width;
      if (w && w > 0) setContainerWidth(w);
    });
    obs.observe(el);
    return () => obs.disconnect();
  }, []);

  const layout = useTrackLayout(containerWidth, view, chromNames, chromLengths);

  const onViewChange = useCallback(
    (v: ViewState) => {
      setView(v);
      onViewChangeProp?.(v);
    },
    [onViewChangeProp],
  );

  const { zoomTo, resetZoom, zoomIn, zoomOut } = useGenomeZoom(svgRef, {
    totalLength: layout.totalLength,
    containerWidth,
    onViewChange,
  });

  // Memoize sorted loci and clusters
  const sortedLoci = useMemo(
    () => sortLociByPosition(loci, layout.offsets),
    [loci, layout.offsets],
  );
  const trackItems = useMemo(
    () => clusterLoci(sortedLoci, layout.bpToPixel, MIN_PIXEL_GAP),
    [sortedLoci, layout.bpToPixel],
  );

  // Zoom to center a locus with padding
  const zoomToLocus = useCallback(
    (locus: TrackLocus) => {
      const chroms = buildChromList(chromNames, chromLengths);
      const { offsets } = chromOffsets(chroms);
      const start = toAbsolute(locus.chr, locus.start, offsets);
      const end = toAbsolute(locus.chr, locus.end, offsets);
      const span = Math.max(end - start, 5_000_000);
      const mid = (start + end) / 2;
      zoomTo(mid - span * 3, mid + span * 3);
    },
    [chromNames, chromLengths, zoomTo],
  );

  // Navigate to prev/next locus
  const navigateLocus = useCallback(
    (direction: 1 | -1) => {
      if (sortedLoci.length === 0) return;
      const currentIdx = selectedLocusId
        ? sortedLoci.findIndex((l) => l.id === selectedLocusId)
        : -1;
      let nextIdx: number;
      if (direction === 1) {
        nextIdx = currentIdx < sortedLoci.length - 1 ? currentIdx + 1 : 0;
      } else {
        nextIdx = currentIdx > 0 ? currentIdx - 1 : sortedLoci.length - 1;
      }
      const next = sortedLoci[nextIdx]!;
      onLocusSelect(next.id);
      zoomToLocus(next);
    },
    [sortedLoci, selectedLocusId, onLocusSelect, zoomToLocus],
  );

  // Zoom to a chromosome (empty string = reset to all)
  const zoomToChrom = useCallback(
    (chr: string) => {
      if (!chr) {
        resetZoom();
        return;
      }
      const chroms = buildChromList(chromNames, chromLengths);
      const { offsets } = chromOffsets(chroms);
      const offset = offsets.get(chr);
      const chrInfo = chroms.find((c) => c.name === chr);
      if (offset === undefined || !chrInfo) return;
      zoomTo(offset, offset + chrInfo.length);
    },
    [chromNames, chromLengths, zoomTo, resetZoom],
  );

  // Zoom to a region
  const zoomToRegion = useCallback(
    (chr: string, start: number, end: number) => {
      const chroms = buildChromList(chromNames, chromLengths);
      const { offsets } = chromOffsets(chroms);
      const absStart = toAbsolute(chr, start, offsets);
      const absEnd = toAbsolute(chr, end, offsets);
      zoomTo(absStart, absEnd);
    },
    [chromNames, chromLengths, zoomTo],
  );

  // Find the nearest track item at a given pixel (x, y). Returns null if nothing within range.
  const hitTest = useCallback(
    (px: number, py: number): { item: TrackItem; dist: number } | null => {
      // Only match in the vertical zone of triangles + labels (above the bar)
      if (py > BAR_Y) return null;
      let best: { item: TrackItem; dist: number } | null = null;
      const hitRadius = 5;
      for (const item of trackItems) {
        let dist: number;
        if (isCluster(item)) {
          dist = Math.abs(px - item.centerPixel);
        } else {
          const locus = item as TrackLocus;
          const midPx = layout.bpToPixel(
            locus.chr,
            (locus.start + locus.end) / 2,
          );
          dist = Math.abs(px - midPx);
        }
        if (dist < hitRadius && (!best || dist < best.dist)) {
          best = { item, dist };
        }
      }
      return best;
    },
    [trackItems, layout],
  );

  // Click: select single locus, zoom into cluster
  const handleSvgClick = useCallback(
    (e: React.MouseEvent<SVGSVGElement>) => {
      const svg = svgRef.current;
      if (!svg) return;
      const rect = svg.getBoundingClientRect();
      const hit = hitTest(e.clientX - rect.left, e.clientY - rect.top);
      if (!hit) return;

      if (isCluster(hit.item)) {
        // Zoom into the cluster region with padding
        const cluster = hit.item;
        const chroms = buildChromList(chromNames, chromLengths);
        const { offsets } = chromOffsets(chroms);
        const absStart = toAbsolute(
          cluster.loci[0]!.chr,
          cluster.loci[0]!.start,
          offsets,
        );
        const absEnd = toAbsolute(
          cluster.loci[cluster.loci.length - 1]!.chr,
          cluster.loci[cluster.loci.length - 1]!.end,
          offsets,
        );
        const span = Math.max(absEnd - absStart, 5_000_000);
        const mid = (absStart + absEnd) / 2;
        zoomTo(mid - span * 2, mid + span * 2);
      } else {
        onLocusSelect((hit.item as TrackLocus).id);
      }
    },
    [hitTest, onLocusSelect, chromNames, chromLengths, zoomTo],
  );

  // Hover: show pointer cursor when over a clickable item
  const handleSvgMouseMove = useCallback(
    (e: React.MouseEvent<SVGSVGElement>) => {
      const svg = svgRef.current;
      if (!svg) return;
      const rect = svg.getBoundingClientRect();
      const hit = hitTest(e.clientX - rect.left, e.clientY - rect.top);
      svg.style.cursor = hit ? "pointer" : "grab";
    },
    [hitTest],
  );

  // Keyboard navigation
  useEffect(() => {
    const handler = (e: KeyboardEvent) => {
      if (
        e.target instanceof HTMLInputElement ||
        e.target instanceof HTMLTextAreaElement ||
        e.target instanceof HTMLSelectElement
      )
        return;

      if (e.key === "ArrowLeft") {
        e.preventDefault();
        navigateLocus(-1);
      } else if (e.key === "ArrowRight") {
        e.preventDefault();
        navigateLocus(1);
      } else if (e.key === "=" || e.key === "+") {
        e.preventDefault();
        zoomIn();
      } else if (e.key === "-") {
        e.preventDefault();
        zoomOut();
      } else if (e.key === "Escape") {
        resetZoom();
      }
    };
    window.addEventListener("keydown", handler);
    return () => window.removeEventListener("keydown", handler);
  }, [navigateLocus, zoomIn, zoomOut, resetZoom]);

  return (
    <div ref={containerRef} className={className} style={{ paddingTop: 120 }}>
      <svg
        ref={svgRef}
        width={containerWidth}
        height={TOTAL_HEIGHT}
        className="select-none overflow-visible"
        onClick={handleSvgClick}
        onMouseMove={handleSvgMouseMove}
      >
        {/* Content group: pointer-events disabled so d3-zoom gets clean events */}
        <g pointerEvents="none">
          <ChromosomeTrack
            layout={layout}
            barY={BAR_Y}
            barHeight={BAR_HEIGHT}
          />
          <LocusMarkers
            items={trackItems}
            layout={layout}
            barY={BAR_Y}
            selectedLocusId={selectedLocusId}
            traitColors={traitColors}
          />
        </g>
        {/* Transparent overlay for d3-zoom events */}
        <rect
          width={containerWidth}
          height={TOTAL_HEIGHT}
          fill="transparent"
        />
      </svg>

      <div className="mt-2">
        <TrackControls
          chromNames={chromNames}
          onChromSelect={zoomToChrom}
          onRegionInput={zoomToRegion}
          onZoomIn={zoomIn}
          onZoomOut={zoomOut}
          onReset={resetZoom}
          onPrevLocus={() => navigateLocus(-1)}
          onNextLocus={() => navigateLocus(1)}
          hasLoci={sortedLoci.length > 0}
        />
      </div>
    </div>
  );
}
