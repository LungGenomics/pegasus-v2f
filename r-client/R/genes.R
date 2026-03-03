#' Search genes by symbol or name
#'
#' Uses DuckDB full-text search when available, falls back to LIKE/ILIKE.
#'
#' @param conn A v2f_connection object.
#' @param query Search string. Empty string returns all genes (paginated).
#' @param limit Maximum number of results (default 100).
#' @return A data.frame of matching genes from `gene_search_index`.
#' @export
search_genes <- function(conn, query = "", limit = 100L) {
  .check_conn(conn)

  if (!.has_table(conn, "gene_search_index")) {
    warning("gene_search_index table not found", call. = FALSE)
    return(.empty_df(
      ensembl_gene_id = "character", gene = "character",
      chromosome = "character", start_position = "integer",
      end_position = "integer", searchable_text = "character"
    ))
  }

  limit <- as.integer(limit)

  if (query == "") {
    return(.run_query(
      conn,
      "SELECT * FROM gene_search_index ORDER BY gene LIMIT ?",
      params = list(limit)
    ))
  }

  if (.is_postgres(conn)) {
    return(.run_query(
      conn,
      "SELECT * FROM gene_search_index WHERE gene ILIKE ? ORDER BY gene LIMIT ?",
      params = list(paste0("%", query, "%"), limit)
    ))
  }

  # DuckDB: try FTS first, fall back to LIKE
  tryCatch(
    .run_query(
      conn,
      paste(
        "SELECT gsi.*, fts.score",
        "FROM fts_main_gene_search_index fts",
        "JOIN gene_search_index gsi",
        "  ON gsi.ensembl_gene_id = fts.ensembl_gene_id",
        "WHERE fts.match_bm25(ensembl_gene_id, ?) IS NOT NULL",
        "ORDER BY fts.score DESC LIMIT ?"
      ),
      params = list(query, limit)
    ),
    error = function(e) {
      .run_query(
        conn,
        paste(
          "SELECT * FROM gene_search_index",
          "WHERE gene LIKE ? OR searchable_text LIKE ?",
          "ORDER BY gene LIMIT ?"
        ),
        params = list(
          paste0("%", query, "%"),
          paste0("%", query, "%"),
          limit
        )
      )
    }
  )
}


#' Get all evidence for a gene
#'
#' Returns both locus-level and gene-level evidence with an
#' `evidence_level` column distinguishing them.
#'
#' @param conn A v2f_connection object.
#' @param gene Gene symbol (e.g. `"HHIP"`).
#' @return A data.frame of evidence rows.
#' @export
gene_evidence <- function(conn, gene) {
  .check_conn(conn)

  has_locus <- .has_table(conn, "locus_gene_evidence")
  has_gene <- .has_table(conn, "gene_evidence")

  empty <- .empty_df(
    locus_id = "character", gene_symbol = "character",
    evidence_category = "character", evidence_stream = "character",
    source_tag = "character", pvalue = "numeric", effect_size = "numeric",
    score = "numeric", tissue = "character", cell_type = "character",
    evidence_level = "character"
  )

  if (!has_locus && !has_gene) {
    warning(
      "No evidence tables found (locus_gene_evidence, gene_evidence)",
      call. = FALSE
    )
    return(empty)
  }

  parts <- list()

  if (has_locus) {
    locus_ev <- .run_query(
      conn,
      paste(
        "SELECT locus_id, gene_symbol, evidence_category, evidence_stream,",
        "source_tag, pvalue, effect_size, score, tissue, cell_type",
        "FROM locus_gene_evidence WHERE gene_symbol = ?"
      ),
      params = list(gene)
    )
    if (nrow(locus_ev) > 0) {
      locus_ev$evidence_level <- "locus"
      parts <- c(parts, list(locus_ev))
    }
  }

  if (has_gene) {
    gene_ev <- .run_query(
      conn,
      paste(
        "SELECT gene_symbol, evidence_category, evidence_type,",
        "source_tag, score, tissue, cell_type",
        "FROM gene_evidence WHERE gene_symbol = ?"
      ),
      params = list(gene)
    )
    if (nrow(gene_ev) > 0) {
      gene_ev$evidence_level <- "gene"
      names(gene_ev)[names(gene_ev) == "evidence_type"] <- "evidence_stream"
      gene_ev$locus_id <- NA_character_
      gene_ev$pvalue <- NA_real_
      gene_ev$effect_size <- NA_real_
      parts <- c(parts, list(gene_ev))
    }
  }

  if (length(parts) == 0) return(empty)

  result <- do.call(rbind, parts)
  # Reorder columns consistently
  cols <- names(empty)
  result[, cols]
}


#' Summarize evidence for a gene
#'
#' Returns evidence category counts and mean scores grouped by evidence level.
#'
#' @param conn A v2f_connection object.
#' @param gene Gene symbol.
#' @return A data.frame with columns: evidence_category, evidence_level, n, mean_score.
#' @export
gene_summary <- function(conn, gene) {
  .check_conn(conn)

  ev <- gene_evidence(conn, gene)

  empty <- .empty_df(
    evidence_category = "character", evidence_level = "character",
    n = "integer", mean_score = "numeric"
  )

  if (nrow(ev) == 0) return(empty)

  # Aggregate in R to avoid backend-specific SQL
  split_key <- paste(ev$evidence_category, ev$evidence_level, sep = "|")
  groups <- split(ev, split_key)

  result <- do.call(rbind, lapply(names(groups), function(key) {
    g <- groups[[key]]
    parts <- strsplit(key, "|", fixed = TRUE)[[1]]
    scores <- g$score[!is.na(g$score)]
    data.frame(
      evidence_category = parts[1],
      evidence_level = parts[2],
      n = nrow(g),
      mean_score = if (length(scores) > 0) mean(scores) else NA_real_,
      stringsAsFactors = FALSE
    )
  }))

  result[order(result$evidence_category, result$evidence_level), ]
}
