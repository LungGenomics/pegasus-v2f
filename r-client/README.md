# PEGASUS V2F — R Client

*Status: planned*

Domain-first R package for analysts working with PEGASUS V2F databases. Provides a read-only interface that returns data frames — no SQL required.

## Planned interface

```r
library(pegasus.v2f)

# Connect to a local or remote database
conn <- v2f_connect("gene.duckdb")
# conn <- v2f_connect("http://localhost:8000")  # or via API

# Search genes
search_genes(conn, "HHIP")

# Get all evidence for a gene
gene_evidence(conn, "HHIP")

# Browse GWAS results
list_traits(conn)
trait_loci(conn, "my_study_fev1")
locus_genes(conn, "HHIP_FEV1")

# Get predicted effector genes
effector_genes(conn, "my_study_fev1")
```

## Design principles

- **Domain-first**: functions named for what analysts think about (genes, evidence, traits), not database operations
- **Data frames out**: every function returns a tibble/data.frame
- **Read-only**: the R client never modifies the database; use the Python CLI for builds and updates
- **Dual access**: connect directly to DuckDB files or via the REST API
