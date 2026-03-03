#' Get genes and scores at a locus
#'
#' Returns the gene score table for a single locus, ordered by integration rank.
#'
#' @param conn A v2f_connection object.
#' @param locus_id Locus identifier.
#' @return A data.frame with gene scores (distance, rank, effector prediction).
#' @export
locus_genes <- function(conn, locus_id) {
  .check_conn(conn)

  if (!.has_table(conn, "locus_gene_scores")) {
    warning("locus_gene_scores table not found", call. = FALSE)
    return(.empty_df(
      gene_symbol = "character", distance_to_lead_kb = "numeric",
      is_nearest_gene = "logical", is_within_locus = "logical",
      integration_score = "numeric", integration_rank = "integer",
      is_predicted_effector = "logical"
    ))
  }

  .run_query(
    conn,
    paste(
      "SELECT gene_symbol, distance_to_lead_kb, is_nearest_gene,",
      "is_within_locus, integration_score, integration_rank,",
      "is_predicted_effector",
      "FROM locus_gene_scores WHERE locus_id = ?",
      "ORDER BY integration_rank"
    ),
    params = list(locus_id)
  )
}


#' Get predicted effector genes for a study
#'
#' Returns genes flagged as predicted effectors across all loci in a study,
#' with locus and trait context.
#'
#' @param conn A v2f_connection object.
#' @param study_id Study identifier.
#' @return A data.frame of effector genes with locus context.
#' @export
effector_genes <- function(conn, study_id) {
  .check_conn(conn)

  needed <- c("locus_gene_scores", "loci", "studies")
  missing <- needed[!vapply(needed, function(t) .has_table(conn, t), logical(1))]

  if (length(missing) > 0) {
    warning("Missing tables: ", paste(missing, collapse = ", "), call. = FALSE)
    return(.empty_df(
      gene_symbol = "character", distance_to_lead_kb = "numeric",
      is_nearest_gene = "logical", is_within_locus = "logical",
      integration_score = "numeric", integration_rank = "integer",
      is_predicted_effector = "logical",
      locus_id = "character", locus_name = "character",
      chromosome = "character", trait = "character"
    ))
  }

  .run_query(
    conn,
    paste(
      "SELECT lgs.gene_symbol, lgs.distance_to_lead_kb, lgs.is_nearest_gene,",
      "lgs.is_within_locus, lgs.integration_score, lgs.integration_rank,",
      "lgs.is_predicted_effector,",
      "lgs.locus_id, l.locus_name, l.chromosome, s.trait",
      "FROM locus_gene_scores lgs",
      "JOIN loci l ON lgs.locus_id = l.locus_id",
      "JOIN studies s ON l.study_id = s.study_id",
      "WHERE s.study_id = ? AND lgs.is_predicted_effector = TRUE",
      "ORDER BY l.chromosome, l.start_position, lgs.integration_rank"
    ),
    params = list(study_id)
  )
}
