test_that("list_traits returns all studies", {
  db <- create_test_db()
  on.exit(close(db))

  result <- list_traits(db)
  expect_s3_class(result, "data.frame")
  expect_equal(nrow(result), 2)
  expect_true(all(c("study_id", "trait", "sample_size", "n_loci") %in% names(result)))
  # Ordered by trait: FEV1 before FVC
  expect_equal(result$trait, c("FEV1", "FVC"))
})

test_that("list_traits warns on missing table", {
  conn <- DBI::dbConnect(duckdb::duckdb(), dbdir = ":memory:")
  db <- structure(
    list(conn = conn, db_path = ":memory:", backend = "duckdb"),
    class = "v2f_connection"
  )
  on.exit(close(db))

  expect_warning(result <- list_traits(db), "studies table not found")
  expect_equal(nrow(result), 0)
  expect_true("study_id" %in% names(result))
})

test_that("trait_loci returns loci for a study", {
  db <- create_test_db()
  on.exit(close(db))

  result <- trait_loci(db, "study_001")
  expect_s3_class(result, "data.frame")
  expect_equal(nrow(result), 2)  # locus_001 and locus_002
  expect_true(all(c("locus_id", "chromosome", "lead_pvalue") %in% names(result)))
})

test_that("trait_loci returns empty for unknown study", {
  db <- create_test_db()
  on.exit(close(db))

  result <- trait_loci(db, "nonexistent_study")
  expect_equal(nrow(result), 0)
})

test_that("trait_loci warns on missing table", {
  conn <- DBI::dbConnect(duckdb::duckdb(), dbdir = ":memory:")
  db <- structure(
    list(conn = conn, db_path = ":memory:", backend = "duckdb"),
    class = "v2f_connection"
  )
  on.exit(close(db))

  expect_warning(result <- trait_loci(db, "study_001"), "loci table not found")
  expect_equal(nrow(result), 0)
})
