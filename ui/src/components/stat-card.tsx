import type { ReactNode } from "react";

export function StatCard({
  label,
  value,
  icon,
  onClick,
}: {
  label: string;
  value: string | number;
  icon?: ReactNode;
  onClick?: () => void;
}) {
  return (
    <div
      className={`stat bg-base-100 rounded-box shadow-sm ${onClick ? "cursor-pointer hover:shadow-md transition-shadow" : ""}`}
      onClick={onClick}
    >
      {icon && <div className="stat-figure text-primary">{icon}</div>}
      <div className="stat-title">{label}</div>
      <div className="stat-value text-2xl">{value}</div>
    </div>
  );
}
