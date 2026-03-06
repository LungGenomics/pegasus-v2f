import { useEffect, useRef, useCallback, type RefObject } from "react";
import { zoom, type ZoomBehavior, zoomIdentity } from "d3-zoom";
import { select } from "d3-selection";
import "d3-transition";
import type { ViewState } from "./types";

type Options = {
  totalLength: number;
  containerWidth: number;
  onViewChange: (view: ViewState) => void;
};

type ZoomControls = {
  zoomTo: (startBp: number, endBp: number) => void;
  resetZoom: () => void;
  zoomIn: () => void;
  zoomOut: () => void;
};

/**
 * Hook that bridges d3-zoom with React.
 *
 * d3-zoom never touches the DOM — it only tracks the zoom transform.
 * On every zoom event, we convert the transform to a ViewState and
 * pass it to React so everything re-renders at correct positions.
 *
 * Transform mapping:
 *   k=1, tx=0 → genome-wide view (all bp fit in containerWidth)
 *   k=N       → zoomed in N times
 *   tx        → pan offset in pixels
 */
export function useGenomeZoom(
  svgRef: RefObject<SVGSVGElement | null>,
  { totalLength, containerWidth, onViewChange }: Options,
): ZoomControls {
  const zoomRef = useRef<ZoomBehavior<SVGSVGElement, unknown> | null>(null);
  const totalRef = useRef(totalLength);
  const widthRef = useRef(containerWidth);
  const viewChangeRef = useRef(onViewChange);
  const rafRef = useRef<number | null>(null);
  const pendingView = useRef<ViewState | null>(null);

  totalRef.current = totalLength;
  widthRef.current = containerWidth;
  viewChangeRef.current = onViewChange;

  const transformToView = useCallback(
    (t: { k: number; x: number }): ViewState => {
      const total = totalRef.current;
      const w = widthRef.current;
      // At k=1, the full genome maps to containerWidth pixels
      // tx shifts the viewport in pixel space (scaled)
      const bpPerPx = total / (w * t.k);
      const startBp = -t.x * bpPerPx;
      const endBp = startBp + total / t.k;
      return {
        startBp: Math.max(0, startBp),
        endBp: Math.min(total, endBp),
      };
    },
    [],
  );

  useEffect(() => {
    const svg = svgRef.current;
    if (!svg || totalLength === 0 || containerWidth === 0) return;

    const maxZoom = Math.max(totalLength / 500_000, 10);

    const zoomBehavior = zoom<SVGSVGElement, unknown>()
      .scaleExtent([1, maxZoom])
      .translateExtent([
        [0, 0],
        [containerWidth, 0],
      ])
      .extent([
        [0, 0],
        [containerWidth, 0],
      ])
      .filter((event) => {
        if (event.type === "dblclick") return false;
        // Reject horizontal-dominant wheel events — handled separately as pan
        if (
          event.type === "wheel" &&
          Math.abs(event.deltaX) > Math.abs(event.deltaY)
        )
          return false;
        return true;
      })
      .on("zoom", (event) => {
        // Throttle React updates to once per animation frame.
        // d3-zoom fires on every wheel event (60-120 fps on trackpads),
        // but React only needs to render once per frame.
        const view = transformToView(event.transform);
        if (rafRef.current === null) {
          rafRef.current = requestAnimationFrame(() => {
            rafRef.current = null;
            viewChangeRef.current(pendingView.current!);
          });
        }
        pendingView.current = view;
      });

    const sel = select(svg);
    sel.call(zoomBehavior);
    sel.on("dblclick.zoom", null);

    zoomRef.current = zoomBehavior;

    // Handle horizontal wheel events as pan (trackpad swipe)
    const handleHorizontalWheel = (e: WheelEvent) => {
      if (Math.abs(e.deltaX) > Math.abs(e.deltaY)) {
        e.preventDefault();
        sel.call(zoomBehavior.translateBy, -e.deltaX / 2, 0);
      }
    };
    svg.addEventListener("wheel", handleHorizontalWheel, { passive: false });

    // Start at genome-wide view
    viewChangeRef.current({ startBp: 0, endBp: totalLength });

    return () => {
      sel.on(".zoom", null);
      svg.removeEventListener("wheel", handleHorizontalWheel);
      if (rafRef.current !== null) {
        cancelAnimationFrame(rafRef.current);
        rafRef.current = null;
      }
    };
  }, [svgRef, totalLength, containerWidth, transformToView]);

  const zoomTo = useCallback(
    (startBp: number, endBp: number) => {
      const svg = svgRef.current;
      const zb = zoomRef.current;
      if (!svg || !zb) return;

      const total = totalRef.current;
      const w = widthRef.current;
      const span = endBp - startBp;
      const k = total / span;
      // tx = -(startBp / bpPerPx) where bpPerPx = total / (w * k)
      const tx = -(startBp * w * k) / total;

      select(svg)
        .transition()
        .duration(750)
        .call(zb.transform, zoomIdentity.translate(tx, 0).scale(k));
    },
    [svgRef],
  );

  const resetZoom = useCallback(() => {
    const svg = svgRef.current;
    const zb = zoomRef.current;
    if (!svg || !zb) return;
    select(svg).transition().duration(500).call(zb.transform, zoomIdentity);
  }, [svgRef]);

  const zoomIn = useCallback(() => {
    const svg = svgRef.current;
    const zb = zoomRef.current;
    if (!svg || !zb) return;
    select(svg).transition().duration(300).call(zb.scaleBy, 2);
  }, [svgRef]);

  const zoomOut = useCallback(() => {
    const svg = svgRef.current;
    const zb = zoomRef.current;
    if (!svg || !zb) return;
    select(svg).transition().duration(300).call(zb.scaleBy, 0.5);
  }, [svgRef]);

  return { zoomTo, resetZoom, zoomIn, zoomOut };
}
