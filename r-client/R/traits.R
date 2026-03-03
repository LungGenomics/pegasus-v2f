#' List all studies/traits
#'
#' @param conn A v2f_connection object.
#' @return A data.frame of studies with columns: study_id, trait,
#'   trait_description, gwas_source, ancestry, sample_size, n_loci.
#' @export
list_traits <- function(conn) {
  .check_conn(conn)

  if (!.has_table(conn, "studies")) {
    warning("studies table not found", call. = FALSE)
    return(.empty_df(
      study_id = "character", trait = "character",
      trait_description = "character", gwas_source = "character",
      ancestry = "character", sample_size = "integer",
      n_loci = "integer"
    ))
  }

  .run_query(
    conn,
    paste(
      "SELECT study_id, trait, trait_description, gwas_source, ancestry,",
      "sample_size, n_loci FROM studies ORDER BY trait"
    )
  )
}


#' Get loci for a study
#'
#' @param conn A v2f_connection object.
#' @param study_id Study identifier.
#' @return A data.frame of loci with genomic coordinates and lead variant info.
#' @export
trait_loci <- function(conn, study_id) {
  .check_conn(conn)

  if (!.has_table(conn, "loci")) {
    warning("loci table not found", call. = FALSE)
    return(.empty_df(
      locus_id = "character", locus_name = "character",
      chromosome = "character", start_position = "numeric",
      end_position = "numeric", lead_variant_id = "character",
      lead_rsid = "character", lead_pvalue = "numeric",
      locus_source = "character", n_candidate_genes = "integer"
    ))
  }

  .run_query(
    conn,
    paste(
      "SELECT locus_id, locus_name, chromosome, start_position, end_position,",
      "lead_variant_id, lead_rsid, lead_pvalue, locus_source, n_candidate_genes",
      "FROM loci WHERE study_id = ? ORDER BY chromosome, start_position"
    ),
    params = list(study_id)
  )
}
