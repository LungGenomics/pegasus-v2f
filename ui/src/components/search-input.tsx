import { useEffect, useRef, useState } from "react";
import { Search } from "lucide-react";

export function SearchInput({
  value,
  onChange,
  placeholder = "Search...",
  debounceMs = 300,
  autoFocus = false,
}: {
  value: string;
  onChange: (value: string) => void;
  placeholder?: string;
  debounceMs?: number;
  autoFocus?: boolean;
}) {
  const [local, setLocal] = useState(value);
  const timer = useRef<ReturnType<typeof setTimeout>>(undefined);

  useEffect(() => {
    setLocal(value);
  }, [value]);

  function handleChange(v: string) {
    setLocal(v);
    clearTimeout(timer.current);
    timer.current = setTimeout(() => onChange(v), debounceMs);
  }

  return (
    <label className="input input-bordered flex items-center gap-2">
      <Search className="size-4 opacity-50" />
      <input
        type="text"
        className="grow"
        placeholder={placeholder}
        value={local}
        onChange={(e) => handleChange(e.target.value)}
        autoFocus={autoFocus}
      />
    </label>
  );
}
