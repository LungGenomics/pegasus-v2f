export function Loading({ text = "Loading..." }: { text?: string }) {
  return (
    <div className="flex items-center gap-2 py-12 justify-center text-base-content/60">
      <span className="loading loading-spinner loading-md" />
      <span>{text}</span>
    </div>
  );
}

export function ErrorAlert({ message }: { message: string }) {
  return (
    <div role="alert" className="alert alert-error">
      <span>{message}</span>
    </div>
  );
}
