#' Get stored database config
#'
#' Reads key-value pairs from the `_pegasus_meta` table embedded during
#' `v2f build`. Returns a named list with keys like `config`, `genome_build`,
#' `package_version`, `build_timestamp`.
#'
#' @param conn A v2f_connection object.
#' @return A named list of config values, or an empty list if no meta table.
#' @export
db_config <- function(conn) {
  .check_conn(conn)

  if (!.has_table(conn, "_pegasus_meta")) {
    warning("_pegasus_meta table not found", call. = FALSE)
    return(list())
  }

  meta <- .run_query(conn, "SELECT key, value FROM _pegasus_meta")
  if (nrow(meta) == 0) return(list())

  result <- as.list(stats::setNames(meta$value, meta$key))

  # Parse the config YAML if yaml package is available
  if ("config" %in% names(result) && requireNamespace("yaml", quietly = TRUE)) {
    tryCatch(
      {
        result$config <- yaml::yaml.load(result$config)
      },
      error = function(e) NULL
    )
  }

  result
}
