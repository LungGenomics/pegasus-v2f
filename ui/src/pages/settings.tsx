import { useState } from "react";
import { useDbStatus, useDbConfig, useTables } from "../api/db";
import { useReconnectDb, useUpdateMeta } from "../api/settings";
import { PageHeader } from "../components/layout/page-header";
import { Loading, ErrorAlert } from "../components/loading";

export function SettingsPage() {
  const statusQ = useDbStatus();
  const configQ = useDbConfig();
  const tablesQ = useTables();
  const updateMeta = useUpdateMeta();
  const reconnect = useReconnectDb();

  const [dbPath, setDbPath] = useState("");

  if (statusQ.isLoading) return <Loading />;
  if (statusQ.error) return <ErrorAlert message={statusQ.error.message} />;

  const status = statusQ.data!;
  const config = configQ.data ?? {};

  return (
    <div>
      <PageHeader title="Settings" />

      {/* Database Info */}
      <section className="card bg-base-100 shadow-sm p-6 mb-6">
        <h2 className="font-semibold mb-3">Database Info</h2>
        <div className="grid grid-cols-2 sm:grid-cols-3 gap-y-2 gap-x-6 text-sm">
          <div>
            <span className="text-base-content/60">Genome Build</span>
            <p className="font-medium">{status.genome_build}</p>
          </div>
          <div>
            <span className="text-base-content/60">Package Version</span>
            <p className="font-medium">{status.package_version}</p>
          </div>
          <div>
            <span className="text-base-content/60">PEGASUS</span>
            <p className="font-medium">{status.has_pegasus ? "Yes" : "No"}</p>
          </div>
          <div>
            <span className="text-base-content/60">Tables</span>
            <p className="font-medium">{tablesQ.data?.length ?? "..."}</p>
          </div>
          <div>
            <span className="text-base-content/60">Studies</span>
            <p className="font-medium">{status.n_studies}</p>
          </div>
          <div>
            <span className="text-base-content/60">Evidence Rows</span>
            <p className="font-medium">{status.n_evidence_rows}</p>
          </div>
        </div>
      </section>

      {/* Config Viewer/Editor */}
      <section className="card bg-base-100 shadow-sm p-6 mb-6">
        <h2 className="font-semibold mb-3">Config</h2>

        {configQ.isLoading ? (
          <Loading text="Loading config..." />
        ) : (
          <>
            <MetaField
              label="Project Name"
              metaKey="project_name"
              value={String(config.project_name ?? "")}
              onSave={(v) =>
                updateMeta.mutate({ key: "project_name", value: v })
              }
              saving={updateMeta.isPending}
            />
            <MetaField
              label="Genome Build"
              metaKey="genome_build"
              value={String(config.genome_build ?? status.genome_build)}
              onSave={(v) =>
                updateMeta.mutate({ key: "genome_build", value: v })
              }
              saving={updateMeta.isPending}
            />

            <details className="mt-4">
              <summary className="cursor-pointer text-sm text-base-content/60">
                Raw Config
              </summary>
              <pre className="mt-2 bg-base-200 p-3 rounded-box text-xs overflow-x-auto">
                {JSON.stringify(config, null, 2)}
              </pre>
            </details>
          </>
        )}
      </section>

      {/* Database Connection */}
      <section className="card bg-base-100 shadow-sm p-6 mb-6">
        <h2 className="font-semibold mb-3">Database Connection</h2>
        <p className="text-sm text-base-content/60 mb-3">
          Change the database backend. This affects all active sessions.
        </p>

        <div className="join w-full max-w-xl">
          <input
            type="text"
            className="input input-bordered join-item grow"
            placeholder="/path/to/file.duckdb or postgresql://..."
            value={dbPath}
            onChange={(e) => setDbPath(e.target.value)}
          />
          <button
            className="btn btn-warning join-item"
            onClick={() => {
              if (
                confirm(
                  "Reconnect to a different database? This affects all sessions.",
                )
              ) {
                reconnect.mutate(dbPath);
              }
            }}
            disabled={!dbPath.trim() || reconnect.isPending}
          >
            {reconnect.isPending ? "Reconnecting..." : "Reconnect"}
          </button>
        </div>

        {reconnect.isError && (
          <div role="alert" className="alert alert-error mt-3">
            <span>{reconnect.error.message}</span>
          </div>
        )}
        {reconnect.isSuccess && (
          <div role="alert" className="alert alert-success mt-3">
            <span>Reconnected successfully.</span>
          </div>
        )}
      </section>
    </div>
  );
}

function MetaField({
  label,
  metaKey: _metaKey,
  value,
  onSave,
  saving,
}: {
  label: string;
  metaKey: string;
  value: string;
  onSave: (value: string) => void;
  saving: boolean;
}) {
  const [editing, setEditing] = useState(false);
  const [local, setLocal] = useState(value);

  if (!editing) {
    return (
      <div className="flex items-center gap-3 py-1">
        <span className="text-sm text-base-content/60 w-32">{label}</span>
        <span className="text-sm font-medium">{value || "-"}</span>
        <button
          className="btn btn-ghost btn-xs"
          onClick={() => {
            setLocal(value);
            setEditing(true);
          }}
        >
          Edit
        </button>
      </div>
    );
  }

  return (
    <div className="flex items-center gap-3 py-1">
      <span className="text-sm text-base-content/60 w-32">{label}</span>
      <input
        type="text"
        className="input input-bordered input-sm"
        value={local}
        onChange={(e) => setLocal(e.target.value)}
        autoFocus
      />
      <button
        className="btn btn-primary btn-xs"
        onClick={() => {
          onSave(local);
          setEditing(false);
        }}
        disabled={saving}
      >
        Save
      </button>
      <button
        className="btn btn-ghost btn-xs"
        onClick={() => setEditing(false)}
      >
        Cancel
      </button>
    </div>
  );
}
