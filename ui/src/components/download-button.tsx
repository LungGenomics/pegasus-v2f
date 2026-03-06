import { Download } from "lucide-react";

export function DownloadButton({
  href,
  label,
  filename,
}: {
  href: string;
  label: string;
  filename?: string;
}) {
  return (
    <a
      href={href}
      download={filename ?? true}
      className="btn btn-sm btn-outline gap-2"
    >
      <Download className="size-4" />
      {label}
    </a>
  );
}
