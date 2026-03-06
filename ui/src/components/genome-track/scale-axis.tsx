import type { TrackLayout } from "./use-track-layout";
import type { ViewState } from "./types";
import { fromAbsolute } from "../../lib/genome-coords";

type Props = {
  layout: TrackLayout;
  view: ViewState;
  axisY: number;
  containerWidth: number;
};

/**
 * Compute "nice" tick interval: round to 1, 2, or 5 × 10^n.
 */
function niceInterval(roughInterval: number): number {
  const magnitude = Math.pow(10, Math.floor(Math.log10(roughInterval)));
  const residual = roughInterval / magnitude;
  if (residual <= 1.5) return magnitude;
  if (residual <= 3.5) return 2 * magnitude;
  return 5 * magnitude;
}

/**
 * Generate tick positions in absolute bp space.
 */
function computeTicks(
  startBp: number,
  endBp: number,
  targetCount = 6,
): number[] {
  const span = endBp - startBp;
  if (span <= 0) return [];
  const interval = niceInterval(span / targetCount);
  const first = Math.ceil(startBp / interval) * interval;
  const ticks: number[] = [];
  for (let bp = first; bp <= endBp; bp += interval) {
    ticks.push(bp);
  }
  return ticks;
}

/**
 * Format a bp value in human-readable form.
 */
function formatBp(bp: number): string {
  const abs = Math.abs(bp);
  if (abs >= 1_000_000_000) {
    const v = bp / 1_000_000_000;
    return Number.isInteger(v) ? `${v}G` : `${v.toFixed(1)}G`;
  }
  if (abs >= 1_000_000) {
    const v = bp / 1_000_000;
    return Number.isInteger(v) ? `${v}M` : `${v.toFixed(1)}M`;
  }
  if (abs >= 1_000) {
    const v = bp / 1_000;
    return Number.isInteger(v) ? `${v}K` : `${v.toFixed(1)}K`;
  }
  return bp.toLocaleString();
}

/**
 * Adaptive scale axis below the chromosome bar.
 */
export function ScaleAxis({ layout, view, axisY, containerWidth }: Props) {
  const ticks = computeTicks(view.startBp, view.endBp, 6);
  const tickHeight = 4;

  return (
    <g className="scale-axis">
      {/* Axis baseline */}
      <line
        x1={0}
        y1={axisY}
        x2={containerWidth}
        y2={axisY}
        stroke="#cbd5e1"
        strokeWidth={0.5}
        opacity={0.5}
      />

      {ticks.map((bp) => {
        // Convert absolute bp to pixel position using the current view
        const px =
          ((bp - view.startBp) / (view.endBp - view.startBp)) * containerWidth;

        // Skip ticks outside visible area
        if (px < -10 || px > containerWidth + 10) return null;

        // Convert to chr-relative position for label
        const { chr, pos } = fromAbsolute(bp, layout.chroms, layout.offsets);
        const chrNum = chr.replace("chr", "");
        const label = `${chrNum}:${formatBp(pos)}`;

        return (
          <g key={bp}>
            <line
              x1={px}
              y1={axisY}
              x2={px}
              y2={axisY + tickHeight}
              stroke="#94a3b8"
              strokeWidth={1}
            />
            <text
              x={px}
              y={axisY + tickHeight + 10}
              textAnchor="middle"
              className="fill-base-content/40"
              fontSize={9}
            >
              {label}
            </text>
          </g>
        );
      })}
    </g>
  );
}
