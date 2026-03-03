test_that("search_genes returns all genes when query is empty", {
  db <- create_test_db()
  on.exit(close(db))

  result <- search_genes(db)
  expect_s3_class(result, "data.frame")
  expect_equal(nrow(result), 5)
  expect_true("gene" %in% names(result))
  expect_true("ensembl_gene_id" %in% names(result))
})

test_that("search_genes filters by gene name (LIKE fallback)", {
  db <- create_test_db()
  on.exit(close(db))

  result <- search_genes(db, "HHIP")
  expect_s3_class(result, "data.frame")
  expect_gte(nrow(result), 1)
  expect_true("HHIP" %in% result$gene)
})

test_that("search_genes respects limit", {
  db <- create_test_db()
  on.exit(close(db))

  result <- search_genes(db, limit = 2)
  expect_equal(nrow(result), 2)
})

test_that("search_genes warns on missing table", {
  conn <- DBI::dbConnect(duckdb::duckdb(), dbdir = ":memory:")
  db <- structure(
    list(conn = conn, db_path = ":memory:", backend = "duckdb"),
    class = "v2f_connection"
  )
  on.exit(close(db))

  expect_warning(result <- search_genes(db, "HHIP"), "not found")
  expect_s3_class(result, "data.frame")
  expect_equal(nrow(result), 0)
  expect_true("gene" %in% names(result))
})

test_that("gene_evidence returns locus and gene level evidence", {
  db <- create_test_db()
  on.exit(close(db))

  result <- gene_evidence(db, "HHIP")
  expect_s3_class(result, "data.frame")
  expect_true("evidence_level" %in% names(result))

  levels <- unique(result$evidence_level)
  expect_true("locus" %in% levels)
  expect_true("gene" %in% levels)

  # HHIP has 3 locus-level + 3 gene-level evidence rows
  expect_equal(sum(result$evidence_level == "locus"), 3)
  expect_equal(sum(result$evidence_level == "gene"), 3)
})

test_that("gene_evidence has consistent columns across levels", {
  db <- create_test_db()
  on.exit(close(db))

  result <- gene_evidence(db, "HHIP")
  expected_cols <- c(
    "locus_id", "gene_symbol", "evidence_category", "evidence_stream",
    "source_tag", "pvalue", "effect_size", "score", "tissue", "cell_type",
    "evidence_level"
  )
  expect_equal(names(result), expected_cols)

  # Gene-level rows should have NA for locus-specific fields
  gene_rows <- result[result$evidence_level == "gene", ]
  expect_true(all(is.na(gene_rows$locus_id)))
  expect_true(all(is.na(gene_rows$pvalue)))
})

test_that("gene_evidence returns empty df for unknown gene", {
  db <- create_test_db()
  on.exit(close(db))

  result <- gene_evidence(db, "NONEXISTENT_GENE")
  expect_s3_class(result, "data.frame")
  expect_equal(nrow(result), 0)
  expect_true("evidence_level" %in% names(result))
})

test_that("gene_evidence warns on missing tables", {
  conn <- DBI::dbConnect(duckdb::duckdb(), dbdir = ":memory:")
  db <- structure(
    list(conn = conn, db_path = ":memory:", backend = "duckdb"),
    class = "v2f_connection"
  )
  on.exit(close(db))

  expect_warning(result <- gene_evidence(db, "HHIP"), "No evidence tables")
  expect_equal(nrow(result), 0)
})

test_that("gene_summary aggregates correctly", {
  db <- create_test_db()
  on.exit(close(db))

  result <- gene_summary(db, "HHIP")
  expect_s3_class(result, "data.frame")
  expect_true(all(c("evidence_category", "evidence_level", "n", "mean_score") %in% names(result)))

  # HHIP has locus-level: QTL(1), COLOC(1), PROX(1); gene-level: KNOW(1), FUNC(1), LIT(1)
  expect_equal(nrow(result), 6)

  qtl_row <- result[result$evidence_category == "QTL" & result$evidence_level == "locus", ]
  expect_equal(qtl_row$n, 1)
  expect_equal(qtl_row$mean_score, 0.95)
})

test_that("gene_summary returns empty df for unknown gene", {
  db <- create_test_db()
  on.exit(close(db))

  result <- gene_summary(db, "NONEXISTENT")
  expect_equal(nrow(result), 0)
  expect_true("mean_score" %in% names(result))
})
