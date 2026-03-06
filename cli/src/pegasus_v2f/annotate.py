"""Gene annotations, search index, and gene table summary."""

from __future__ import annotations

import logging
from typing import Any

import pandas as pd

from pegasus_v2f.db import is_postgres, raw_table_name, write_table
from pegasus_v2f.report import Report

logger = logging.getLogger(__name__)

# Allowed chromosomes for gene annotations
VALID_CHROMOSOMES = {str(i) for i in range(1, 23)} | {"X", "Y", "MT"}


def create_gene_annotations(
    conn: Any,
    genes: list[str],
    config: dict,
    report: Report | None = None,
) -> None:
    """Fetch gene annotations from Ensembl and write to DB.

    In PEGASUS mode (``genes`` table exists), inserts into the ``genes``
    table with column mapping (gene → gene_symbol, display_name → gene_name).
    In legacy mode, writes to ``gene_annotations`` as before.
    """
    ga_config = config.get("gene_annotations", {})
    genome_build = config.get("database", {}).get("genome_build", "hg38")

    if report:
        report.counters["genes_requested"] = len(genes)

    is_stub = False
    try:
        df = _fetch_ensembl_genes(genes, ga_config)
    except Exception as e:
        logger.warning(f"Ensembl fetch failed: {e}. Creating stub annotations.")
        is_stub = True
        df = pd.DataFrame({
            "gene": genes,
            "ensembl_gene_id": [None] * len(genes),
            "gene_name": [None] * len(genes),
            "chromosome": [None] * len(genes),
            "start_position": [None] * len(genes),
            "end_position": [None] * len(genes),
            "strand": [None] * len(genes),
        })
        if report:
            report.error(
                "ensembl_failed",
                f"Ensembl fetch failed: {e}. All {len(genes)} genes have stub annotations (NULL coordinates). "
                "Scoring will find 0 candidate genes per locus.",
            )

    df["genome_build"] = genome_build

    # Filter to valid chromosomes
    before_filter = len(df)
    if "chromosome" in df.columns:
        df = df[df["chromosome"].isin(VALID_CHROMOSOMES) | df["chromosome"].isna()]
    filtered_count = before_filter - len(df)

    if report:
        report.counters["genes_found"] = len(df)
        if not is_stub:
            not_found = len(genes) - len(df) - filtered_count
            if not_found > 0:
                report.warning(
                    "genes_not_found",
                    "gene symbols not found in Ensembl",
                    count=not_found,
                )
        if filtered_count > 0:
            report.warning(
                "invalid_chromosome",
                "genes filtered out (non-standard chromosome)",
                count=filtered_count,
            )

    # PEGASUS mode: insert into genes table
    if _has_table(conn, "genes"):
        _insert_into_genes(conn, df)
    else:
        # Legacy mode: write to gene_annotations
        write_table(conn, "gene_annotations", df)


def _has_table(conn: Any, table_name: str) -> bool:
    """Check if a table exists in the database."""
    try:
        if is_postgres(conn):
            cur = conn.cursor()
            cur.execute(
                "SELECT 1 FROM information_schema.tables "
                "WHERE table_schema = 'public' AND table_name = %s",
                (table_name,),
            )
            result = cur.fetchone() is not None
            cur.close()
            return result
        else:
            result = conn.execute(
                "SELECT 1 FROM information_schema.tables WHERE table_name = ?",
                [table_name],
            ).fetchone()
            return result is not None
    except Exception:
        return False


def _insert_into_genes(conn: Any, df: pd.DataFrame) -> None:
    """Insert gene annotation data into the PEGASUS ``genes`` table.

    Uses ON CONFLICT DO UPDATE so genes already present (e.g. from
    a prior source) get their coordinates filled in.
    """
    if df.empty:
        return

    # Map legacy column names → PEGASUS schema
    insert_df = df.rename(columns={"gene": "gene_symbol"})
    if "gene_name" not in insert_df.columns:
        insert_df["gene_name"] = None

    cols = [
        "gene_symbol", "ensembl_gene_id", "gene_name",
        "chromosome", "start_position", "end_position",
        "strand", "genome_build",
    ]
    insert_df = insert_df[[c for c in cols if c in insert_df.columns]]

    upsert_sql = """
        INSERT INTO genes (gene_symbol, ensembl_gene_id, gene_name,
            chromosome, start_position, end_position, strand, genome_build)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT (gene_symbol) DO UPDATE SET
            ensembl_gene_id = COALESCE(EXCLUDED.ensembl_gene_id, genes.ensembl_gene_id),
            gene_name = COALESCE(EXCLUDED.gene_name, genes.gene_name),
            chromosome = COALESCE(EXCLUDED.chromosome, genes.chromosome),
            start_position = COALESCE(EXCLUDED.start_position, genes.start_position),
            end_position = COALESCE(EXCLUDED.end_position, genes.end_position),
            strand = COALESCE(EXCLUDED.strand, genes.strand),
            genome_build = COALESCE(EXCLUDED.genome_build, genes.genome_build)
    """

    if is_postgres(conn):
        # PostgreSQL uses %s placeholders
        pg_sql = upsert_sql.replace("?", "%s")
        cur = conn.cursor()
        for _, row in insert_df.iterrows():
            cur.execute(pg_sql, tuple(row.get(c) for c in cols))
        conn.commit()
        cur.close()
    else:
        # DuckDB — batch via executemany
        conn.executemany(
            upsert_sql,
            [tuple(row.get(c) for c in cols) for _, row in insert_df.iterrows()],
        )

    n = len(insert_df)
    logger.info(f"Inserted/updated {n} genes in PEGASUS genes table")


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
                        "gene_name": info.get("display_name"),
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

    # Determine gene reference table: prefer PEGASUS genes, fallback to gene_annotations
    if _has_table(conn, "genes"):
        gene_table = "genes"
        gene_col = "gene_symbol"
        dedup_cte = f"""
        WITH deduplicated_genes AS (
            SELECT
                ensembl_gene_id,
                {gene_col} AS gene,
                chromosome,
                start_position,
                end_position
            FROM {gene_table}
            WHERE ensembl_gene_id IS NOT NULL
        )"""
    else:
        gene_table = "gene_annotations"
        gene_col = "gene"
        dedup_cte = f"""
        WITH deduplicated_genes AS (
            SELECT
                ensembl_gene_id,
                MIN({gene_col}) AS gene,
                MIN(chromosome) AS chromosome,
                MIN(start_position) AS start_position,
                MIN(end_position) AS end_position
            FROM {gene_table}
            WHERE ensembl_gene_id IS NOT NULL
            GROUP BY ensembl_gene_id
        )"""

    # Build SQL
    aliases = {s["name"]: f"t{i}" for i, s in enumerate(searchable)}
    join_clauses = []
    agg_columns = []
    search_parts = ["COALESCE(g.gene, '')"]

    for s in searchable:
        alias = aliases[s["name"]]
        table = raw_table_name(s["name"])
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
    {dedup_cte}
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
            SELECT gene_symbol FROM evidence
            UNION
            SELECT gene_symbol FROM scored_evidence
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
        FROM evidence
        GROUP BY gene_symbol
    ),
    score_info AS (
        SELECT
            gene_symbol,
            MIN(integration_rank) AS best_rank,
            MAX(CASE WHEN is_predicted_effector THEN 1 ELSE 0 END) AS is_any_effector,
            COUNT(DISTINCT locus_id) AS n_loci
        FROM scored_evidence
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




