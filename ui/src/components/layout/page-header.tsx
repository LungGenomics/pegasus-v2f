import type { ReactNode } from "react";
import { Link } from "react-router";

export function PageHeader({
  title,
  description,
  actions,
  breadcrumbs,
}: {
  title: string;
  description?: string;
  actions?: ReactNode;
  breadcrumbs?: { label: string; to?: string }[];
}) {
  return (
    <div className="mb-6">
      {breadcrumbs && breadcrumbs.length > 0 && (
        <div className="breadcrumbs text-sm mb-2">
          <ul>
            {breadcrumbs.map((b, i) => (
              <li key={i}>
                {b.to ? <Link to={b.to}>{b.label}</Link> : b.label}
              </li>
            ))}
          </ul>
        </div>
      )}
      <div className="flex items-center justify-between gap-4">
        <div>
          <h1 className="text-2xl font-bold">{title}</h1>
          {description && (
            <p className="text-base-content/60 mt-1">{description}</p>
          )}
        </div>
        {actions && <div className="flex gap-2">{actions}</div>}
      </div>
    </div>
  );
}
