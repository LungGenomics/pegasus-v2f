import { Link, useParams } from "react-router";
import { useGene, useGeneEvidence, useGeneScores } from "../api/genes";
import { PageHeader } from "../components/layout/page-header";
import { DataTable, type Column } from "../components/data-table";
import { EvidenceBadge } from "../components/evidence-badge";
import { Loading, ErrorAlert } from "../components/loading";
import { formatCoordinate, formatPvalue, formatScore } from "../lib/format";
import type { GeneEvidence, GeneScore } from "../api/types";

const scoreColumns: Column<GeneScore & Record<string, unknown>>[] = [
  {
    key: "study_id",
    header: "Study",
    render: (row) => (
      <Link
        to={`/studies/${encodeURIComponent(row.study_id)}`}
        className="link link-primary"
      >
        {row.study_id}
      </Link>
    ),
  },
  {
    key: "locus_name",
    header: "Locus",
    render: (row) => (
      <Link
        to={`/loci/${encodeURIComponent(row.locus_id)}`}
        className="link"
      >
        {row.locus_name}
      </Link>
    ),
  },
  {
    key: "distance_to_lead_kb",
    header: "Distance (kb)",
    render: (row) => String(row.distance_to_lead_kb),
  },
  {
    key: "integration_score",
    header: "Score",
    render: (row) => formatScore(row.integration_score),
  },
  { key: "integration_rank", header: "Rank" },
  {
    key: "is_predicted_effector",
    header: "PEG",
    render: (row) =>
      row.is_predicted_effector === true ||
      row.is_predicted_effector === "true" ? (
        <span className="badge badge-sm badge-success">PEG</span>
      ) : (
        "-"
      ),
  },
];

function evidenceColumns(
  level: "locus" | "gene",
): Column<GeneEvidence & Record<string, unknown>>[] {
  const base: Column<GeneEvidence & Record<string, unknown>>[] = [
    {
      key: "evidence_category",
      header: "Category",
      render: (row) => <EvidenceBadge category={row.evidence_category} />,
    },
    {
      key: level === "locus" ? "evidence_stream" : "evidence_type",
      header: level === "locus" ? "Stream" : "Type",
    },
    { key: "source_tag", header: "Source" },
  ];

  if (level === "locus") {
    base.push(
      {
        key: "pvalue",
        header: "P-value",
        render: (row) => (row.pvalue ? formatPvalue(row.pvalue) : "-"),
      },
      {
        key: "score",
        header: "Score",
        render: (row) => formatScore(row.score),
      },
      { key: "tissue", header: "Tissue" },
    );
  } else {
    base.push(
      { key: "trait", header: "Trait" },
      {
        key: "score",
        header: "Score",
        render: (row) => formatScore(row.score),
      },
      { key: "tissue", header: "Tissue" },
    );
  }

  return base;
}

export function GeneDetailPage() {
  const { gene } = useParams<{ gene: string }>();
  const geneQ = useGene(gene ?? "");
  const evidenceQ = useGeneEvidence(gene ?? "");
  const scoresQ = useGeneScores(gene ?? "");

  if (geneQ.isLoading) return <Loading />;
  if (geneQ.error) return <ErrorAlert message={geneQ.error.message} />;
  if (!geneQ.data) return null;

  const g = geneQ.data;
  const locusEvidence = (evidenceQ.data ?? []).filter(
    (e) => e.evidence_level === "locus",
  );
  const geneEvidence = (evidenceQ.data ?? []).filter(
    (e) => e.evidence_level === "gene",
  );

  return (
    <div>
      <PageHeader
        title={g.gene_symbol}
        description={g.gene_name}
        breadcrumbs={[
          { label: "Genes", to: "/genes" },
          { label: g.gene_symbol },
        ]}
      />

      <div className="flex flex-wrap gap-3 mb-6 text-sm">
        <span className="badge badge-outline">{g.ensembl_gene_id}</span>
        {g.chromosome && (
          <span className="badge badge-outline">
            {formatCoordinate(
              g.chromosome,
              g.start_position,
              g.end_position,
            )}
          </span>
        )}
        {g.strand && (
          <span className="badge badge-outline">
            Strand: {g.strand}
          </span>
        )}
      </div>

      <section className="mb-8">
        <h2 className="text-lg font-semibold mb-3">Locus Scores</h2>
        {scoresQ.isLoading ? (
          <Loading text="Loading scores..." />
        ) : (
          <DataTable
            data={(scoresQ.data ?? []) as (GeneScore & Record<string, unknown>)[]}
            columns={scoreColumns}
            emptyMessage="No locus scores"
          />
        )}
      </section>

      <section className="mb-8">
        <h2 className="text-lg font-semibold mb-3">Locus-Level Evidence</h2>
        {evidenceQ.isLoading ? (
          <Loading text="Loading evidence..." />
        ) : (
          <DataTable
            data={locusEvidence as (GeneEvidence & Record<string, unknown>)[]}
            columns={evidenceColumns("locus")}
            emptyMessage="No locus-level evidence"
          />
        )}
      </section>

      <section className="mb-8">
        <h2 className="text-lg font-semibold mb-3">Gene-Level Evidence</h2>
        {evidenceQ.isLoading ? (
          <Loading text="Loading evidence..." />
        ) : (
          <DataTable
            data={geneEvidence as (GeneEvidence & Record<string, unknown>)[]}
            columns={evidenceColumns("gene")}
            emptyMessage="No gene-level evidence"
          />
        )}
      </section>
    </div>
  );
}
