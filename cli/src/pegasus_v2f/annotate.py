"""Gene annotations, search index, and gene table summary."""

from __future__ import annotations

import logging
from typing import Any

import pandas as pd

from pegasus_v2f.db import is_postgres, write_table

logger = logging.getLogger(__name__)

# Allowed chromosomes for gene annotations
VALID_CHROMOSOMES = {str(i) for i in range(1, 23)} | {"X", "Y", "MT"}


def create_gene_annotations(
    conn: Any,
    genes: list[str],
    config: dict,
) -> None:
    """Fetch gene annotations from Ensembl via biomaRt-style REST API and write to DB.

    Falls back to a simple lookup table if Ensembl is unavailable.
    """
    ga_config = config.get("gene_annotations", {})
    genome_build = config.get("database", {}).get("genome_build", "hg38")

    try:
        df = _fetch_ensembl_genes(genes, ga_config)
    except Exception as e:
        logger.warning(f"Ensembl fetch failed: {e}. Creating stub annotations.")
        df = pd.DataFrame({
            "gene": genes,
            "ensembl_gene_id": [None] * len(genes),
            "chromosome": [None] * len(genes),
            "start_position": [None] * len(genes),
            "end_position": [None] * len(genes),
            "strand": [None] * len(genes),
        })

    df["genome_build"] = genome_build

    # Filter to valid chromosomes
    if "chromosome" in df.columns:
        df = df[df["chromosome"].isin(VALID_CHROMOSOMES) | df["chromosome"].isna()]

    write_table(conn, "gene_annotations", df)


def _fetch_ensembl_genes(genes: list[str], ga_config: dict) -> pd.DataFrame:
    """Fetch gene info from Ensembl REST API."""
    import httpx

    # Use Ensembl REST API (no biomaRt dependency)
    base_url = "https://rest.ensembl.org"

    # Batch lookup — POST /lookup/symbol/homo_sapiens with symbols
    # Ensembl REST has a limit of 1000 per request
    results = []
    batch_size = 1000

    for i in range(0, len(genes), batch_size):
        batch = [g for g in genes[i : i + batch_size] if g and str(g) != "nan"]
        if not batch:
            continue

        resp = httpx.post(
            f"{base_url}/lookup/symbol/homo_sapiens",
            json={"symbols": batch},
            headers={"Content-Type": "application/json", "Accept": "application/json"},
            timeout=60,
        )

        if resp.status_code == 200:
            data = resp.json()
            for symbol, info in data.items():
                if isinstance(info, dict) and "id" in info:
                    results.append({
                        "gene": symbol,
                        "ensembl_gene_id": info.get("id"),
                        "chromosome": str(info.get("seq_region_name", "")),
                        "start_position": info.get("start"),
                        "end_position": info.get("end"),
                        "strand": info.get("strand"),
                    })
        else:
            logger.warning(f"Ensembl batch lookup returned {resp.status_code}")

    if not results:
        raise RuntimeError("No results from Ensembl")

    return pd.DataFrame(results)


def create_search_index(
    conn: Any,
    sources: list[dict],
    loaded_tables: list[str],
    config: dict,
) -> None:
    """Create the gene_search_index table by joining all source tables.

    This is a denormalized view for UI search.
    """
    # Filter to searchable sources that were actually loaded
    searchable = [
        s for s in sources
        if s.get("include_in_search", True) and s["name"] in loaded_tables
    ]

    if not searchable:
        logger.warning("No searchable sources — skipping search index")
        return

    # Build SQL
    aliases = {s["name"]: f"t{i}" for i, s in enumerate(searchable)}
    join_clauses = []
    agg_columns = []
    search_parts = ["COALESCE(g.gene, '')"]

    for s in searchable:
        alias = aliases[s["name"]]
        table = s["name"]
        join_clauses.append(f'LEFT JOIN "{table}" {alias} ON g.gene = {alias}.gene')

        # Display columns
        for col in s.get("display_columns", []):
            safe_name = f"{table}_{col}"
            agg_columns.append(
                f"STRING_AGG(DISTINCT CAST({alias}.{col} AS VARCHAR), ', ') AS {safe_name}"
            )

        # Search columns
        for col in s.get("search_columns", []):
            search_parts.append(
                f"COALESCE(STRING_AGG(DISTINCT CAST({alias}.{col} AS VARCHAR), ' '), '')"
            )

    # Aggregated columns from search_index config
    si_config = config.get("search_index", {})
    for ac in si_config.get("aggregated_columns", []):
        source_name = ac["source"]
        if source_name in aliases:
            alias = aliases[source_name]
            agg_columns.append(
                f"STRING_AGG(DISTINCT CAST({alias}.{ac['column']} AS VARCHAR), ', ') AS {ac['name']}"
            )

    searchable_text = " || ' ' || ".join(search_parts)
    agg_sql = ",\n    ".join(agg_columns) if agg_columns else ""
    if agg_sql:
        agg_sql = ",\n    " + agg_sql
    joins_sql = "\n".join(join_clauses)

    sql = f"""
    DROP TABLE IF EXISTS gene_search_index;

    CREATE TABLE gene_search_index AS
    WITH deduplicated_genes AS (
        SELECT
            ensembl_gene_id,
            MIN(gene) AS gene,
            MIN(chromosome) AS chromosome,
            MIN(start_position) AS start_position,
            MIN(end_position) AS end_position
        FROM gene_annotations
        WHERE ensembl_gene_id IS NOT NULL
        GROUP BY ensembl_gene_id
    )
    SELECT
        g.ensembl_gene_id,
        g.gene,
        g.chromosome,
        g.start_position,
        g.end_position{agg_sql},
        {searchable_text} AS searchable_text
    FROM deduplicated_genes g
    {joins_sql}
    GROUP BY g.ensembl_gene_id, g.gene, g.chromosome, g.start_position, g.end_position
    """

    for statement in sql.split(";"):
        statement = statement.strip()
        if statement:
            conn.execute(statement)

    # DuckDB full-text search index
    if not is_postgres(conn):
        try:
            conn.execute("INSTALL fts")
            conn.execute("LOAD fts")
            conn.execute(
                "PRAGMA create_fts_index('gene_search_index', 'ensembl_gene_id', 'searchable_text', overwrite=1)"
            )
        except Exception as e:
            logger.warning(f"FTS index creation failed: {e}")

    if is_postgres(conn):
        conn.commit()


def create_gene_table_summary(
    conn: Any,
    sources: list[dict],
    loaded_tables: list[str],
) -> None:
    """Create gene_table_summary showing which genes appear in which tables."""
    # Filter to sources with a gene column that were loaded
    eligible = [
        s for s in sources
        if s.get("gene_column", "gene") and s["name"] in loaded_tables
    ]

    if not eligible:
        logger.warning("No eligible sources — skipping gene table summary")
        return

    union_parts = []
    for s in eligible:
        table = s["name"]
        display = s.get("display_name", table)
        union_parts.append(
            f"SELECT '{table}' AS table_name, '{display}' AS display_name, "
            f"gene, COUNT(*) AS row_count "
            f'FROM "{table}" WHERE gene IS NOT NULL GROUP BY gene'
        )

    union_sql = "\nUNION ALL\n".join(union_parts)

    conn.execute("DROP TABLE IF EXISTS gene_table_summary")
    conn.execute(f"CREATE TABLE gene_table_summary AS\n{union_sql}")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_gene_table_gene ON gene_table_summary(gene)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_gene_table_table ON gene_table_summary(table_name)")

    if is_postgres(conn):
        conn.commit()


def create_pegasus_search_index(conn: Any) -> None:
    """Create gene_search_index from genes + evidence tables (PEGASUS mode).

    Builds a denormalized search table from the PEGASUS schema tables
    rather than from raw source tables.
    """
    conn.execute("DROP TABLE IF EXISTS gene_search_index")

    sql = """
    CREATE TABLE gene_search_index AS
    WITH all_genes AS (
        SELECT DISTINCT gene_symbol FROM (
            SELECT gene_symbol FROM locus_gene_evidence
            UNION
            SELECT gene_symbol FROM gene_evidence
            UNION
            SELECT gene_symbol FROM locus_gene_scores
        ) sub
    ),
    gene_info AS (
        SELECT
            ag.gene_symbol,
            g.ensembl_gene_id,
            g.chromosome,
            g.start_position,
            g.end_position
        FROM all_genes ag
        LEFT JOIN genes g ON ag.gene_symbol = g.gene_symbol
    ),
    evidence_cats AS (
        SELECT
            gene_symbol,
            STRING_AGG(DISTINCT evidence_category, ', ' ORDER BY evidence_category) AS evidence_categories
        FROM (
            SELECT gene_symbol, evidence_category FROM locus_gene_evidence
            UNION ALL
            SELECT gene_symbol, evidence_category FROM gene_evidence
        ) sub
        GROUP BY gene_symbol
    ),
    score_info AS (
        SELECT
            gene_symbol,
            MIN(integration_rank) AS best_rank,
            MAX(CASE WHEN is_predicted_effector THEN 1 ELSE 0 END) AS is_any_effector,
            COUNT(DISTINCT locus_id) AS n_loci
        FROM locus_gene_scores
        GROUP BY gene_symbol
    )
    SELECT
        gi.gene_symbol,
        gi.ensembl_gene_id,
        gi.chromosome,
        gi.start_position,
        gi.end_position,
        ec.evidence_categories,
        si.best_rank,
        si.is_any_effector,
        si.n_loci,
        COALESCE(gi.gene_symbol, '') || ' ' ||
        COALESCE(gi.ensembl_gene_id, '') || ' ' ||
        COALESCE(ec.evidence_categories, '') AS searchable_text
    FROM gene_info gi
    LEFT JOIN evidence_cats ec ON gi.gene_symbol = ec.gene_symbol
    LEFT JOIN score_info si ON gi.gene_symbol = si.gene_symbol
    """

    conn.execute(sql)

    # DuckDB full-text search index
    if not is_postgres(conn):
        try:
            conn.execute("INSTALL fts")
            conn.execute("LOAD fts")
            conn.execute(
                "PRAGMA create_fts_index('gene_search_index', 'gene_symbol', 'searchable_text', overwrite=1)"
            )
        except Exception as e:
            logger.warning(f"FTS index creation failed: {e}")

    if is_postgres(conn):
        conn.commit()

    logger.info("Created PEGASUS search index")


def create_gene_evidence_summary(conn: Any) -> None:
    """Create gene_evidence_summary showing which genes have which evidence types.

    Replaces gene_table_summary for PEGASUS builds — summarizes from
    evidence tables rather than raw source tables.
    """
    conn.execute("DROP TABLE IF EXISTS gene_evidence_summary")

    sql = """
    CREATE TABLE gene_evidence_summary AS
    SELECT
        gene_symbol,
        evidence_category,
        source_tag,
        'locus' AS evidence_level,
        COUNT(*) AS record_count
    FROM locus_gene_evidence
    GROUP BY gene_symbol, evidence_category, source_tag
    UNION ALL
    SELECT
        gene_symbol,
        evidence_category,
        source_tag,
        'gene' AS evidence_level,
        COUNT(*) AS record_count
    FROM gene_evidence
    GROUP BY gene_symbol, evidence_category, source_tag
    """

    conn.execute(sql)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_ges_gene ON gene_evidence_summary(gene_symbol)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_ges_cat ON gene_evidence_summary(evidence_category)")

    if is_postgres(conn):
        conn.commit()

    logger.info("Created gene evidence summary")


