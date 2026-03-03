test_that("query_db executes raw SQL", {
  db <- create_test_db()
  on.exit(close(db))

  result <- query_db(db, "SELECT gene_symbol FROM genes ORDER BY gene_symbol")
  expect_s3_class(result, "data.frame")
  expect_equal(nrow(result), 5)
  expect_equal(result$gene_symbol, c("AGER", "FAM13A", "GSTCD", "HHIP", "HTR4"))
})

test_that("query_db supports parameterized queries", {
  db <- create_test_db()
  on.exit(close(db))

  result <- query_db(db, "SELECT * FROM genes WHERE chromosome = ?", params = list("chr4"))
  expect_equal(nrow(result), 3)  # HHIP, FAM13A, GSTCD
})

test_that("list_tables returns all tables with row counts", {
  db <- create_test_db()
  on.exit(close(db))

  result <- list_tables(db)
  expect_s3_class(result, "data.frame")
  expect_true(all(c("table_name", "row_count") %in% names(result)))
  expect_true("genes" %in% result$table_name)

  genes_row <- result[result$table_name == "genes", ]
  expect_equal(genes_row$row_count, 5L)
})

test_that("list_sources reads data_sources table", {
  db <- create_test_db()
  on.exit(close(db))

  result <- list_sources(db)
  expect_s3_class(result, "data.frame")
  expect_equal(nrow(result), 2)
  expect_true("source_tag" %in% names(result))
})

test_that("list_sources falls back to source_metadata on legacy db", {
  db <- create_legacy_test_db()
  on.exit(close(db))

  result <- list_sources(db)
  expect_s3_class(result, "data.frame")
  expect_equal(nrow(result), 1)
  expect_true("table_name" %in% names(result))
})

test_that("list_sources warns on empty db", {
  conn <- DBI::dbConnect(duckdb::duckdb(), dbdir = ":memory:")
  db <- structure(
    list(conn = conn, db_path = ":memory:", backend = "duckdb"),
    class = "v2f_connection"
  )
  on.exit(close(db))

  expect_warning(result <- list_sources(db), "No data source table")
  expect_equal(nrow(result), 0)
})

test_that("db_config returns meta key-value pairs", {
  db <- create_test_db()
  on.exit(close(db))

  result <- db_config(db)
  expect_type(result, "list")
  expect_true("genome_build" %in% names(result))
  expect_equal(result$genome_build, "GRCh38")
  expect_true("package_version" %in% names(result))
})

test_that("db_config parses config YAML when yaml available", {
  skip_if_not_installed("yaml")
  db <- create_test_db()
  on.exit(close(db))

  result <- db_config(db)
  # config should be parsed into a list if yaml is available
  expect_type(result$config, "list")
  expect_equal(result$config$name, "test_lung_v2f")
})

test_that("db_config warns on missing meta table", {
  conn <- DBI::dbConnect(duckdb::duckdb(), dbdir = ":memory:")
  db <- structure(
    list(conn = conn, db_path = ":memory:", backend = "duckdb"),
    class = "v2f_connection"
  )
  on.exit(close(db))

  expect_warning(result <- db_config(db), "_pegasus_meta table not found")
  expect_equal(length(result), 0)
})
