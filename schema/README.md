# PEGASUS V2F — Schema Reference

*Status: planned*

Documentation of the PEGASUS database schema, controlled vocabularies, and data model.

## Database tables

| Table | Description |
|---|---|
| `studies` | GWAS studies, one per trait |
| `loci` | Genomic regions from GWAS (curated or auto-clumped) |
| `genes` | Gene annotations from Ensembl |
| `variants` | Variant positions from sumstats or source data |
| `locus_gene_evidence` | Variant/locus-level evidence (the PEGASUS matrix) |
| `gene_evidence` | Gene-level annotations not tied to specific loci |
| `locus_gene_scores` | Integration scores and rankings per locus-gene pair |
| `data_sources` | Provenance tracking for all loaded sources |

## PEGASUS evidence categories

The 22 controlled abbreviations from the PEGASUS standard (EBI SPOT):

`QTL`, `COLOC`, `GWAS`, `PROX`, `KNOW`, `EXP`, `PROT`, `PATH`, `MOD`, `OMIM`, `CLIN`, `FUNC`, `PHENO`, `EPIGEN`, `TF`, `CHROM`, `CONS`, `RARE`, `BURDEN`, `PPI`, `DRUG`, `OTHER`

See `cli/src/pegasus_v2f/pegasus_schema.py` for the full DDL and category definitions.
