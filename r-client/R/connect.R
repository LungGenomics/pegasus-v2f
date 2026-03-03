#' Connect to a V2F database
#'
#' Opens a read-only connection to a DuckDB file or PostgreSQL database built
#' by the `v2f` Python CLI.
#'
#' @param db Path to a DuckDB file, a PostgreSQL connection string
#'   (`postgresql://...`), or `NULL` for auto-discovery. Auto-discovery checks
#'   the `V2F_DATABASE_URL` environment variable, then walks up from the working
#'   directory looking for `v2f.yaml` and opens `.v2f/gene.duckdb`.
#' @return A `v2f_connection` object (S3 wrapper around a DBI connection).
#' @export
#' @examples
#' \dontrun{
#' conn <- v2f_connect("path/to/.v2f/gene.duckdb")
#' conn <- v2f_connect("postgresql://user:pass@host/dbname")
#' conn <- v2f_connect()  # auto-discover
#' }
v2f_connect <- function(db = NULL) {
  if (is.null(db)) {
    db <- Sys.getenv("V2F_DATABASE_URL", unset = "")
    if (db == "") {
      db <- .discover_db()
    }
  }

  if (startsWith(db, "postgresql://") || startsWith(db, "postgres://")) {
    if (!requireNamespace("RPostgres", quietly = TRUE)) {
      stop(
        "RPostgres package required for PostgreSQL connections. ",
        "Install with: install.packages('RPostgres')",
        call. = FALSE
      )
    }
    conn <- DBI::dbConnect(RPostgres::Postgres(), db)
    backend <- "postgresql"
  } else {
    if (!file.exists(db)) {
      stop("Database file not found: ", db, call. = FALSE)
    }
    conn <- DBI::dbConnect(duckdb::duckdb(), dbdir = db, read_only = TRUE)
    backend <- "duckdb"
  }

  structure(
    list(conn = conn, db_path = db, backend = backend),
    class = "v2f_connection"
  )
}

#' @export
print.v2f_connection <- function(x, ...) {
  cat("<v2f_connection>\n")
  cat("  Backend:", x$backend, "\n")
  cat("  Database:", x$db_path, "\n")
  if (DBI::dbIsValid(x$conn)) {
    n_tables <- length(DBI::dbListTables(x$conn))
    cat("  Tables:", n_tables, "\n")
  } else {
    cat("  Status: disconnected\n")
  }
  invisible(x)
}

#' @export
close.v2f_connection <- function(con, ...) {
  if (DBI::dbIsValid(con$conn)) {
    DBI::dbDisconnect(con$conn)
  }
  invisible(con)
}

#' Walk up directories looking for v2f.yaml
#' @noRd
.discover_db <- function(start = getwd()) {
  dir <- normalizePath(start, mustWork = FALSE)
  while (TRUE) {
    yaml_path <- file.path(dir, "v2f.yaml")
    if (file.exists(yaml_path)) {
      db_path <- file.path(dir, ".v2f", "gene.duckdb")
      if (file.exists(db_path)) {
        return(db_path)
      }
      stop(
        "Found v2f.yaml at ", dir,
        " but no .v2f/gene.duckdb database. Run 'v2f build' first.",
        call. = FALSE
      )
    }
    parent <- dirname(dir)
    if (parent == dir) break
    dir <- parent
  }
  stop(
    "No v2f.yaml found. Provide a db path or set V2F_DATABASE_URL.",
    call. = FALSE
  )
}
