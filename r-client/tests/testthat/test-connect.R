test_that("v2f_connect validates db path", {
  expect_error(v2f_connect("/nonexistent/file.duckdb"), "not found")
})

test_that("v2f_connect requires RPostgres for postgresql://", {
  # This test only validates the error path if RPostgres is not installed
  # On systems with RPostgres, it would try to actually connect
  skip_if(requireNamespace("RPostgres", quietly = TRUE))
  expect_error(v2f_connect("postgresql://localhost/testdb"), "RPostgres")
})

test_that("print.v2f_connection shows backend and table count", {
  db <- create_test_db()
  on.exit(close(db))

  output <- capture.output(print(db))
  expect_true(any(grepl("duckdb", output)))
  expect_true(any(grepl("Tables:", output)))
})

test_that("close.v2f_connection disconnects", {
  db <- create_test_db()
  expect_true(DBI::dbIsValid(db$conn))
  close(db)
  expect_false(DBI::dbIsValid(db$conn))
})

test_that(".check_conn rejects non-v2f objects", {
  expect_error(search_genes(list(conn = NULL), "HHIP"), "v2f_connection")
})

test_that(".discover_db fails without v2f.yaml", {
  tmp <- tempdir()
  expect_error(
    pegasus.v2f:::.discover_db(start = tmp),
    "No v2f.yaml found"
  )
})
