#' Execute raw SQL and return a data.frame
#'
#' Escape hatch for queries not covered by the convenience functions.
#'
#' @param conn A v2f_connection object.
#' @param sql SQL query string (use `?` placeholders for parameters).
#' @param params List of parameter values (optional).
#' @return A data.frame of results.
#' @export
#' @examples
#' \dontrun{
#' query_db(conn, "SELECT * FROM genes WHERE chromosome = ?", params = list("chr4"))
#' }
query_db <- function(conn, sql, params = NULL) {
  .check_conn(conn)
  .run_query(conn, sql, params = params)
}


#' List all tables with row counts
#'
#' @param conn A v2f_connection object.
#' @return A data.frame with columns: table_name, row_count.
#' @export
list_tables <- function(conn) {
  .check_conn(conn)
  tables <- DBI::dbListTables(conn$conn)
  if (length(tables) == 0) {
    return(.empty_df(table_name = "character", row_count = "integer"))
  }
  counts <- vapply(tables, function(t) {
    r <- DBI::dbGetQuery(conn$conn, paste0('SELECT COUNT(*) AS n FROM "', t, '"'))
    as.integer(r$n)
  }, integer(1), USE.NAMES = FALSE)
  data.frame(table_name = tables, row_count = counts, stringsAsFactors = FALSE)
}


#' List data sources
#'
#' Returns metadata from the `data_sources` table (PEGASUS databases) or
#' `source_metadata` table (legacy databases).
#'
#' @param conn A v2f_connection object.
#' @return A data.frame of data source metadata.
#' @export
list_sources <- function(conn) {
  .check_conn(conn)

  if (.has_table(conn, "data_sources")) {
    return(.run_query(
      conn,
      paste(
        "SELECT source_tag, source_name, source_type, evidence_category,",
        "is_integrated, version, url, citation, record_count",
        "FROM data_sources ORDER BY source_tag"
      )
    ))
  }

  if (.has_table(conn, "source_metadata")) {
    return(.run_query(
      conn,
      paste(
        "SELECT table_name, display_name, description, data_type, source_type",
        "FROM source_metadata ORDER BY table_name"
      )
    ))
  }

  warning("No data source table found (data_sources or source_metadata)", call. = FALSE)
  .empty_df(
    source_tag = "character", source_name = "character",
    source_type = "character", evidence_category = "character",
    is_integrated = "logical", version = "character",
    url = "character", citation = "character",
    record_count = "integer"
  )
}
