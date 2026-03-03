# Internal helpers for pegasus.v2f
# Not exported — used by all other modules.

#' Validate a v2f_connection object
#' @noRd
.check_conn <- function(conn) {
  if (!inherits(conn, "v2f_connection")) {
    stop(
      "Expected a v2f_connection object. Use v2f_connect() to create one.",
      call. = FALSE
    )
  }
  if (!DBI::dbIsValid(conn$conn)) {
    stop("Database connection is closed.", call. = FALSE)
  }
  invisible(conn)
}

#' Check if the connection is to PostgreSQL
#' @noRd
.is_postgres <- function(conn) {
  inherits(conn$conn, "PqConnection")
}

#' Check if a table exists in the database
#' @noRd
.has_table <- function(conn, table_name) {
  table_name %in% DBI::dbListTables(conn$conn)
}

#' Execute a parameterized query and return a data.frame
#'
#' Handles DBI placeholder differences: DuckDB uses `?`, PostgreSQL uses `$1`.
#' Write all SQL with `?` placeholders — this function converts for PostgreSQL.
#' @noRd
.run_query <- function(conn, sql, params = NULL) {
  if (!is.null(params) && .is_postgres(conn)) {
    for (i in seq_along(params)) {
      sql <- sub("?", paste0("$", i), sql, fixed = TRUE)
    }
  }
  if (is.null(params)) {
    DBI::dbGetQuery(conn$conn, sql)
  } else {
    DBI::dbGetQuery(conn$conn, sql, params = params)
  }
}

#' Create an empty data.frame with typed columns
#'
#' Used for graceful degradation when tables don't exist.
#' @param ... Named arguments: column_name = "type" (character, numeric, integer, logical)
#' @noRd
.empty_df <- function(...) {
  cols <- list(...)
  df <- as.data.frame(
    lapply(cols, function(type) vector(type, length = 0)),
    stringsAsFactors = FALSE
  )
  df
}
