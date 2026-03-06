import { Inbox } from "lucide-react";

export function EmptyState({
  title = "No data",
  message,
}: {
  title?: string;
  message?: string;
}) {
  return (
    <div className="flex flex-col items-center gap-2 py-16 text-base-content/50">
      <Inbox className="size-10" />
      <p className="font-medium">{title}</p>
      {message && <p className="text-sm">{message}</p>}
    </div>
  );
}
