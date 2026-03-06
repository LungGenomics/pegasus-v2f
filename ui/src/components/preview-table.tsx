export function PreviewTable({
  rows,
  maxRows = 5,
}: {
  rows: Record<string, unknown>[];
  maxRows?: number;
}) {
  if (rows.length === 0) return null;

  const columns = Object.keys(rows[0]!);
  const display = rows.slice(0, maxRows);

  return (
    <div className="overflow-x-auto">
      <table className="table table-xs">
        <thead>
          <tr>
            {columns.map((col) => (
              <th key={col}>{col}</th>
            ))}
          </tr>
        </thead>
        <tbody>
          {display.map((row, i) => (
            <tr key={i}>
              {columns.map((col) => (
                <td key={col} className="max-w-48 truncate">
                  {String(row[col] ?? "")}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
      {rows.length > maxRows && (
        <p className="text-xs text-base-content/50 mt-1">
          Showing {maxRows} of {rows.length} rows
        </p>
      )}
    </div>
  );
}
