import { useState } from "react";
import { ZoomIn, ZoomOut, Maximize, ChevronLeft, ChevronRight } from "lucide-react";

type Props = {
  chromNames: string[];
  onChromSelect: (chr: string) => void;
  onRegionInput: (chr: string, start: number, end: number) => void;
  onZoomIn: () => void;
  onZoomOut: () => void;
  onReset: () => void;
  onPrevLocus: () => void;
  onNextLocus: () => void;
  hasLoci: boolean;
};

/**
 * Navigation controls for the genome track:
 * chromosome dropdown, region input, zoom buttons, locus stepper.
 */
export function TrackControls({
  chromNames,
  onChromSelect,
  onRegionInput,
  onZoomIn,
  onZoomOut,
  onReset,
  onPrevLocus,
  onNextLocus,
  hasLoci,
}: Props) {
  const [regionText, setRegionText] = useState("");

  const handleRegionSubmit = () => {
    const parsed = parseRegion(regionText);
    if (parsed) {
      onRegionInput(parsed.chr, parsed.start, parsed.end);
      setRegionText("");
    }
  };

  return (
    <div className="flex items-center gap-2 flex-wrap">
      {/* Chromosome selector */}
      <select
        className="select select-bordered select-sm w-28"
        onChange={(e) => {
          if (e.target.value) onChromSelect(e.target.value);
        }}
        defaultValue=""
      >
        <option value="">All</option>
        {chromNames.map((name) => (
          <option key={name} value={name}>
            {name.replace("chr", "Chr ")}
          </option>
        ))}
      </select>

      {/* Region input */}
      <div className="join">
        <input
          type="text"
          className="input input-bordered input-sm join-item w-48"
          placeholder="chr2:150M-160M"
          value={regionText}
          onChange={(e) => setRegionText(e.target.value)}
          onKeyDown={(e) => {
            if (e.key === "Enter") handleRegionSubmit();
          }}
        />
        <button
          className="btn btn-sm btn-ghost join-item"
          onClick={handleRegionSubmit}
          disabled={!regionText}
        >
          Go
        </button>
      </div>

      {/* Zoom controls */}
      <div className="join">
        <button
          className="btn btn-sm btn-ghost join-item"
          onClick={onZoomOut}
          title="Zoom out (-)"
        >
          <ZoomOut size={16} />
        </button>
        <button
          className="btn btn-sm btn-ghost join-item"
          onClick={onZoomIn}
          title="Zoom in (+)"
        >
          <ZoomIn size={16} />
        </button>
        <button
          className="btn btn-sm btn-ghost join-item"
          onClick={onReset}
          title="Reset (Esc)"
        >
          <Maximize size={16} />
        </button>
      </div>

      {/* Locus stepper */}
      {hasLoci && (
        <div className="join">
          <button
            className="btn btn-sm btn-ghost join-item"
            onClick={onPrevLocus}
            title="Previous locus (←)"
          >
            <ChevronLeft size={16} />
          </button>
          <button
            className="btn btn-sm btn-ghost join-item"
            onClick={onNextLocus}
            title="Next locus (→)"
          >
            <ChevronRight size={16} />
          </button>
        </div>
      )}
    </div>
  );
}

/**
 * Parse region strings like "chr2:150000000-160000000" or "chr2:150M-160M".
 */
function parseRegion(
  text: string,
): { chr: string; start: number; end: number } | null {
  const match = text
    .trim()
    .match(/^(chr[\dXY]+):([0-9.]+[MmKk]?)-([0-9.]+[MmKk]?)$/i);
  if (!match) return null;

  const chr = match[1]!.toLowerCase().replace("chr", "chr");
  const start = parseBp(match[2]!);
  const end = parseBp(match[3]!);

  if (isNaN(start) || isNaN(end) || start >= end) return null;
  return { chr: `chr${chr.replace("chr", "")}`, start, end };
}

function parseBp(s: string): number {
  const lower = s.toLowerCase();
  if (lower.endsWith("m")) return parseFloat(s) * 1_000_000;
  if (lower.endsWith("k")) return parseFloat(s) * 1_000;
  return parseInt(s, 10);
}
