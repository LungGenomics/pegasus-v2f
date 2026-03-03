test_that("locus_genes returns scored genes at a locus", {
  db <- create_test_db()
  on.exit(close(db))

  result <- locus_genes(db, "locus_001")
  expect_s3_class(result, "data.frame")
  expect_equal(nrow(result), 2)  # HHIP and FAM13A
  expect_true(all(c("gene_symbol", "integration_score", "integration_rank",
                     "is_predicted_effector") %in% names(result)))

  # Ordered by rank: HHIP (rank 1) then FAM13A (rank 2)
  expect_equal(result$gene_symbol, c("HHIP", "FAM13A"))
  expect_equal(result$integration_rank, c(1L, 2L))
})

test_that("locus_genes returns empty for unknown locus", {
  db <- create_test_db()
  on.exit(close(db))

  result <- locus_genes(db, "nonexistent_locus")
  expect_equal(nrow(result), 0)
})

test_that("locus_genes warns on missing table", {
  conn <- DBI::dbConnect(duckdb::duckdb(), dbdir = ":memory:")
  db <- structure(
    list(conn = conn, db_path = ":memory:", backend = "duckdb"),
    class = "v2f_connection"
  )
  on.exit(close(db))

  expect_warning(result <- locus_genes(db, "locus_001"), "not found")
  expect_equal(nrow(result), 0)
  expect_true("gene_symbol" %in% names(result))
})

test_that("effector_genes returns only predicted effectors", {
  db <- create_test_db()
  on.exit(close(db))

  result <- effector_genes(db, "study_001")
  expect_s3_class(result, "data.frame")
  # study_001 has 2 loci: HHIP (effector) and GSTCD (effector)
  expect_equal(nrow(result), 2)
  expect_true(all(result$is_predicted_effector))
  expect_true(all(c("locus_name", "chromosome", "trait") %in% names(result)))
})

test_that("effector_genes returns empty for study with no effectors", {
  db <- create_test_db()
  on.exit(close(db))

  # study_002 has AGER (effector) and HTR4 (not effector)
  result <- effector_genes(db, "study_002")
  expect_equal(nrow(result), 1)
  expect_equal(result$gene_symbol, "AGER")
})

test_that("effector_genes warns on missing tables", {
  conn <- DBI::dbConnect(duckdb::duckdb(), dbdir = ":memory:")
  db <- structure(
    list(conn = conn, db_path = ":memory:", backend = "duckdb"),
    class = "v2f_connection"
  )
  on.exit(close(db))

  expect_warning(result <- effector_genes(db, "study_001"), "Missing tables")
  expect_equal(nrow(result), 0)
})
