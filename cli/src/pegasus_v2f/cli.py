"""Main CLI entry point for v2f."""

import functools

import rich_click as click

from pegasus_v2f import __version__

click.rich_click.USE_RICH_MARKUP = True
click.rich_click.GROUP_ARGUMENTS_OPTIONS = True
click.rich_click.STYLE_COMMANDS_TABLE_COLUMN_WIDTH_RATIO = (1, 6)
click.rich_click.COMMAND_GROUPS = {
    "v2f": [
        {"name": "Resources", "commands": ["source", "study", "table"]},
        {"name": "Workflow", "commands": ["init", "rescore", "serve", "sync", "status"]},
        {"name": "Query & Export", "commands": ["query", "config", "export"]},
    ],
    "v2f source": [
        {"name": "Commands", "commands": ["list", "add", "show", "update", "remove"]},
    ],
    "v2f study": [
        {"name": "Commands", "commands": ["list", "add", "show", "preview", "set", "remove"]},
    ],
}


# --- Error handling ---

def handle_errors(f):
    """Safety-net decorator: converts unhandled exceptions to ClickException."""
    @functools.wraps(f)
    def wrapper(*args, **kwargs):
        try:
            return f(*args, **kwargs)
        except click.ClickException:
            raise
        except click.Abort:
            raise SystemExit(0)
        except SystemExit:
            raise
        except KeyboardInterrupt:
            raise SystemExit(130)
        except Exception as e:
            raise click.ClickException(str(e))
    return wrapper


# --- Global options ---

@click.group(context_settings={"help_option_names": ["-h", "--help"]})
@click.version_option(__version__, prog_name="v2f")
@click.option("--db", default=None, help="Database path or PostgreSQL connection string.")
@click.option("--project", default=None, help="Override project root directory.")
@click.option("--json", "json_output", is_flag=True, help="Output structured JSON.")
@click.option("--quiet", is_flag=True, help="Suppress progress output.")
@click.pass_context
def cli(ctx, db, project, json_output, quiet):
    """PEGASUS V2F — gene prioritization pipeline and database engine."""
    ctx.ensure_object(dict)
    ctx.obj["db"] = db
    ctx.obj["project"] = project
    ctx.obj["json_output"] = json_output
    ctx.obj["quiet"] = quiet


# =====================================================================
# Workflow commands (top-level)
# =====================================================================

    # NOTE: `v2f build` has been removed. Evidence is now loaded incrementally
    # via `v2f source add`, loci via `v2f study add`, and scoring via `v2f rescore`.
    # The old monolithic build pipeline (pipeline.py) is retained for programmatic
    # use but is no longer exposed as a CLI command.


@cli.command()
@handle_errors
@click.argument("url", required=False)
@click.pass_context
def init(ctx, url):
    """Initialize a new project (optionally from a git URL)."""
    from pathlib import Path
    from pegasus_v2f.project import init_project

    dest = Path.cwd() if not url else Path.cwd() / Path(url).stem
    try:
        root = init_project(dest, url=url)
    except FileExistsError as e:
        raise click.ClickException(str(e))
    click.echo(f"Initialized v2f project at {root}")


@cli.command()
@handle_errors
@click.pass_context
def status(ctx):
    """Show project, database, and sync status."""
    from pegasus_v2f.project import find_project_root, project_status
    from pegasus_v2f.sync import sync_status as _sync_status
    from rich.console import Console

    root = find_project_root(ctx.obj.get("project"))
    if not root:
        raise click.ClickException("Not in a v2f project (no v2f.yaml found)")

    info = project_status(root)
    console = Console()

    console.print(f"[bold]Project:[/bold]  {info['project_root']}")
    console.print(f"[bold]Config:[/bold]   v2f.yaml ({info['sources_count']} data sources)")
    if info["has_local_overrides"]:
        console.print("[bold]Local:[/bold]    .v2f/local.yaml (overrides active)")

    db = info["database"]
    if db["backend"] == "duckdb":
        if db["exists"]:
            size_mb = db["size_bytes"] / (1024 * 1024)
            console.print(f"[bold]Database:[/bold] {db['path']} ({size_mb:.1f} MB)")
        else:
            console.print("[bold]Database:[/bold] [dim]not built yet[/dim]")
    else:
        console.print("[bold]Database:[/bold] PostgreSQL")

    # Git/sync status
    git = _sync_status(root)
    if git["is_git"]:
        parts = []
        if git["behind"] > 0:
            parts.append(f"[yellow]{git['behind']} behind[/yellow]")
        if git["ahead"] > 0:
            parts.append(f"{git['ahead']} ahead")
        if git["dirty"]:
            parts.append(f"[yellow]{len(git['dirty_files'])} uncommitted[/yellow]")
        if not parts:
            parts.append("[green]up to date[/green]")

        branch = git["branch"] or "detached"
        console.print(f"[bold]Remote:[/bold]   {branch} — {', '.join(parts)}")
    else:
        console.print("[bold]Remote:[/bold]   [dim]not a git repo[/dim]")


@cli.command()
@handle_errors
@click.option("--build", "auto_build", is_flag=True, help="Auto-rebuild after pull.")
@click.option("--push", is_flag=True, help="Push local config changes.")
@click.option("--message", "-m", default=None, help="Commit message for --push.")
@click.pass_context
def sync(ctx, auto_build, push, message):
    """Sync config with remote repository."""
    import logging
    from pegasus_v2f.project import find_project_root
    from pegasus_v2f.sync import sync_pull, sync_push, sync_status
    from rich.console import Console

    logging.basicConfig(level=logging.WARNING if ctx.obj.get("quiet") else logging.INFO)
    console = Console()

    root = find_project_root(ctx.obj.get("project"))
    if not root:
        raise click.ClickException("Not in a v2f project (no v2f.yaml found)")

    try:
        if push:
            result = sync_push(root, message=message)
            if result["pushed"]:
                console.print(f"[green]Pushed[/green] {len(result['files_staged'])} file(s)")
                for f in result["files_staged"]:
                    console.print(f"  {f}")
            else:
                console.print("[dim]Nothing to push[/dim]")
            return

        # Default: pull
        st = sync_status(root)
        if not st["is_git"]:
            raise click.ClickException("Not a git repository — nothing to sync")

        if st["behind"] == 0 and st["ahead"] == 0:
            console.print("[green]Up to date[/green]")
            return

        if st["ahead"] > 0:
            console.print(f"[yellow]{st['ahead']} commit(s) ahead[/yellow] — use --push to push")

        if st["behind"] > 0:
            result = sync_pull(root)
            if result["pulled"]:
                console.print(f"[green]Pulled[/green] {result['commits_pulled']} commit(s)")
                for f in result["files_changed"]:
                    console.print(f"  {f}")

                if result["config_changed"] and auto_build:
                    console.print("\n[bold]Config changed — rebuilding...[/bold]")
                    from pegasus_v2f.config import read_config
                    from pegasus_v2f.db import open_db
                    from pegasus_v2f.pipeline import build_db

                    config = read_config(root)
                    db_arg = ctx.obj.get("db")
                    with open_db(db=db_arg, config=config) as conn:
                        build_result = build_db(conn, config, project_root=root, overwrite=True)
                    console.print(
                        f"Built {build_result['sources_loaded']}/{build_result['sources_total']} sources, "
                        f"{build_result['genes_found']} genes"
                    )
                elif result["config_changed"]:
                    console.print("\n[yellow]Config changed[/yellow] — run [bold]v2f rescore[/bold] to update scoring")
            else:
                console.print("[green]Up to date[/green]")

    except RuntimeError as e:
        raise click.ClickException(str(e))


@cli.command()
@handle_errors
@click.argument("sql")
@click.option("--format", "fmt", type=click.Choice(["csv", "json", "table"]), default="table")
@click.pass_context
def query(ctx, sql, fmt):
    """Execute an ad hoc SQL query."""
    import json
    import csv
    import sys
    from pegasus_v2f.project import find_project_root
    from pegasus_v2f.config import read_config
    from pegasus_v2f.db import open_db, is_postgres
    from rich.console import Console
    from rich.table import Table as RichTable

    db_arg = ctx.obj.get("db")
    config = None
    root = None
    if not db_arg:
        root = find_project_root(ctx.obj.get("project"))
        if root:
            config = read_config(root)

    with open_db(db=db_arg, config=config, read_only=True, project_root=root) as conn:
        try:
            if is_postgres(conn):
                cur = conn.cursor()
                cur.execute(sql)
                columns = [desc[0] for desc in cur.description] if cur.description else []
                rows = cur.fetchall() if columns else []
                cur.close()
            else:
                result = conn.execute(sql)
                columns = [desc[0] for desc in result.description] if result.description else []
                rows = result.fetchall() if columns else []
        except Exception as e:
            raise click.ClickException(str(e))

    if not columns:
        click.echo("Query returned no results.")
        return

    if fmt == "json" or ctx.obj.get("json_output"):
        data = [{col: val for col, val in zip(columns, row)} for row in rows]
        click.echo(json.dumps(data, indent=2, default=str))
    elif fmt == "csv":
        writer = csv.writer(sys.stdout)
        writer.writerow(columns)
        writer.writerows(rows)
    else:
        console = Console()
        table = RichTable()
        for col in columns:
            table.add_column(col)
        for row in rows:
            table.add_row(*(str(v) if v is not None else "" for v in row))
        console.print(table)


@cli.command()
@handle_errors
@click.option("--port", default=8000, help="Port to serve on.")
@click.option("--host", default="127.0.0.1", help="Host to bind to.")
@click.option("--reload", is_flag=True, help="Enable auto-reload for development.")
@click.pass_context
def serve(ctx, port, host, reload):
    """Start FastAPI + React UI server."""
    import uvicorn
    from pegasus_v2f.project import find_project_root
    from pegasus_v2f.config import read_config
    from pegasus_v2f_api.app import create_app

    db_arg = ctx.obj.get("db")
    config = None
    root = find_project_root(ctx.obj.get("project"))
    if root:
        config = read_config(root)

    app = create_app(db=db_arg, config=config, project_root=root)

    click.echo(f"Starting v2f server at http://{host}:{port}")
    uvicorn.run(app, host=host, port=port, reload=reload)


@cli.command()
@handle_errors
@click.pass_context
def rebuild(ctx):
    """Delete and rebuild the database from v2f.yaml config.

    Recreates the DB from scratch: schema, studies (with loci from stored
    loci_source), data sources, gene annotations, and scoring.
    """
    import logging
    from pegasus_v2f.project import find_project_root
    from pegasus_v2f.config import read_config, get_study_list, get_data_sources
    from pegasus_v2f.db import get_connection
    from pegasus_v2f.pegasus_schema import create_pegasus_schema
    from pegasus_v2f.db_schema import drop_all_tables, has_tables

    logging.basicConfig(level=logging.WARNING if ctx.obj.get("quiet") else logging.INFO)

    db_arg = ctx.obj.get("db")
    root = find_project_root(ctx.obj.get("project"))
    if not root:
        raise click.ClickException("Not in a v2f project (no v2f.yaml found)")

    config = read_config(root)

    from pegasus_v2f.report import Report, render_report
    report = Report(operation="rebuild")

    conn = get_connection(db=db_arg, config=config, project_root=root)
    try:
        if has_tables(conn):
            if not click.confirm("This will delete and rebuild the entire database. Continue?", default=False):
                raise SystemExit(0)
            drop_all_tables(conn)

        create_pegasus_schema(conn)

        # --- Rebuild studies ---
        studies = get_study_list(config)
        locus_def = config.get("pegasus", {}).get("locus_definition", {})
        total_loci = 0

        for study_cfg in studies:
            study_name = study_cfg["id_prefix"]
            traits = study_cfg.get("traits", [])
            loci_source = study_cfg.get("loci_source")

            if not loci_source:
                click.echo(f"  Skipping study '{study_name}' — no loci_source in config")
                continue

            from pegasus_v2f.study_management import add_study as _add_study

            study_report = report.child(f"study:{study_name}")
            result = _add_study(
                conn,
                study_name=study_name,
                traits=traits,
                loci_file=None,
                loci_df=None,
                loci_source=loci_source,
                loci_sheet=study_cfg.get("loci_sheet"),
                loci_skip=study_cfg.get("loci_skip"),
                gwas_source=study_cfg.get("gwas_source"),
                ancestry=study_cfg.get("ancestry"),
                sex=study_cfg.get("sex"),
                sample_size=study_cfg.get("sample_size"),
                doi=study_cfg.get("doi"),
                year=study_cfg.get("year"),
                genome_build=study_cfg.get("genome_build", config.get("database", {}).get("genome_build", "hg38")),
                gene_column=study_cfg.get("gene_column"),
                sentinel_column=study_cfg.get("sentinel_column"),
                pvalue_column=study_cfg.get("pvalue_column"),
                rsid_column=study_cfg.get("rsid_column"),
                window_kb=locus_def.get("window_kb", 500),
                merge_distance_kb=locus_def.get("merge_distance_kb", 250),
                cache_dir=root / ".v2f",
                config_path=None,  # Don't re-write yaml during rebuild
                report=study_report,
            )
            total_loci += result["n_loci"]
            click.echo(f"  Study '{study_name}': {result['n_loci']} loci from {result['n_sentinels']} sentinels")

        # --- Rebuild sources ---
        sources = get_data_sources(config)
        data_dir = root / "data" / "raw"
        if not data_dir.exists():
            data_dir = root
        total_sources = 0

        for source_def in sources:
            name = source_def["name"]
            try:
                from pegasus_v2f import sources as src_mod
                source_report = report.child(f"source:{name}")
                rows = src_mod.add_source(conn, source_def, data_dir=data_dir, config=config,
                                          no_score=True, report=source_report)
                total_sources += 1
                click.echo(f"  Source '{name}': {rows} rows")
            except Exception as e:
                click.echo(f"  Source '{name}' failed: {e}", err=True)
                report.error("source_failed", f"{name}: {e}")

        # --- Gene annotations ---
        from pegasus_v2f.annotate import create_gene_annotations, create_pegasus_search_index

        gene_rows = conn.execute("SELECT DISTINCT gene_symbol FROM evidence").fetchall()
        all_genes = [r[0] for r in gene_rows if r[0]]
        if all_genes:
            annotate_report = report.child("annotate")
            create_gene_annotations(conn, all_genes, config, report=annotate_report)
            click.echo(f"  Annotated {len(all_genes)} genes")

        # --- Score ---
        from pegasus_v2f.scoring import materialize_scored_evidence
        score_report = report.child("scoring")
        n_scored = materialize_scored_evidence(conn, config, report=score_report)
        click.echo(f"  Scored {n_scored} locus-gene pairs")

        # --- Search index ---
        create_pegasus_search_index(conn)

        # --- Build metadata ---
        from pegasus_v2f.config import config_to_yaml, get_database_config
        from pegasus_v2f.db_meta import write_build_meta
        db_config = get_database_config(config)
        write_build_meta(conn, config_to_yaml(config), genome_build=db_config.get("genome_build", "hg38"))

        click.echo(f"\nRebuilt: {len(studies)} studies, {total_loci} loci, {total_sources} sources, {n_scored} scored pairs")
        if ctx.obj.get("json_output"):
            render_report(report, json_mode=True)
        elif report.has_warnings:
            render_report(report)

    finally:
        conn.close()


@cli.command()
@handle_errors
@click.pass_context
def rescore(ctx):
    """Re-run integration scoring (without full rebuild)."""
    import logging
    from pegasus_v2f.project import find_project_root
    from pegasus_v2f.config import read_config
    from pegasus_v2f.db import open_db
    from pegasus_v2f.scoring import materialize_scored_evidence

    logging.basicConfig(level=logging.WARNING if ctx.obj.get("quiet") else logging.INFO)

    db_arg = ctx.obj.get("db")
    config = None
    root = find_project_root(ctx.obj.get("project"))
    if root:
        config = read_config(root)

    if not config or not config.get("pegasus"):
        raise click.ClickException("No pegasus config found — rescore requires a PEGASUS build")

    from pegasus_v2f.report import Report, render_report
    report = Report(operation="rescore")

    with open_db(db=db_arg, config=config, project_root=root) as conn:
        n = materialize_scored_evidence(conn, config, report=report)

    click.echo(f"Scored {n} locus-gene pairs")
    if ctx.obj.get("json_output"):
        render_report(report, json_mode=True)
    elif report.has_warnings:
        render_report(report)


# =====================================================================
# Source group
# =====================================================================

@cli.group()
@click.pass_context
def source(ctx):
    """Manage data sources."""
    pass


@source.command("list")
@handle_errors
@click.pass_context
def source_list(ctx):
    """List data sources."""
    from pegasus_v2f.project import find_project_root
    from pegasus_v2f.config import read_config
    from pegasus_v2f.db import open_db
    from pegasus_v2f import sources as src_mod
    from rich.console import Console
    from rich.table import Table as RichTable

    db_arg = ctx.obj.get("db")
    config = None
    root = find_project_root(ctx.obj.get("project"))
    if root:
        config = read_config(root)

    with open_db(db=db_arg, config=config, read_only=True, project_root=root) as conn:
        sources = src_mod.list_sources(conn)

    if not sources:
        click.echo("No sources configured. Use 'v2f source add' to add one.")
        return

    console = Console()
    table = RichTable(title="Data Sources")
    table.add_column("Name", style="bold")
    table.add_column("Type")
    table.add_column("Display Name")

    for s in sources:
        table.add_row(s["name"], s.get("source_type", ""), s.get("display_name", s["name"]))

    console.print(table)


def _pick_column(label, col_names, default=None):
    """Prompt user to select a column from a list."""
    import questionary
    result = questionary.select(f"{label}:", choices=col_names, default=default).ask()
    if result is None:
        raise SystemExit(0)
    return result


def _columns_from_preview(preview_df, skip_rows=0):
    """Build column info dicts from a preview DataFrame (header=None format).

    The preview DataFrame has integer column indices and the header text
    lives in the row at index `skip_rows`.
    """
    import pandas as pd
    if skip_rows < len(preview_df):
        header_row = preview_df.iloc[skip_rows]
        col_names = [str(v) if pd.notna(v) else f"col_{i}" for i, v in enumerate(header_row)]
        data_rows = preview_df.iloc[skip_rows + 1:]
    else:
        col_names = [str(c) for c in preview_df.columns]
        data_rows = preview_df

    columns = []
    for i, col in enumerate(col_names):
        samples = []
        for _, row in data_rows.head(3).iterrows():
            val = row.iloc[i] if i < len(row) else None
            if pd.notna(val):
                samples.append(str(val))
        col_type = "text"
        if samples:
            try:
                [float(s) for s in samples]
                col_type = "numeric"
            except ValueError:
                pass
        columns.append({"name": col, "type": col_type, "sample_values": samples})
    return columns


def _apply_suggested_fixes(fixes):
    """Prompt user to accept/skip suggested transformation fixes.

    Returns list of accepted transformation dicts.
    """
    import questionary

    actionable = [f for f in fixes if f.transformation is not None]
    if not actionable:
        return []

    click.echo()
    choice = questionary.select(
        "Apply suggested transformations?",
        choices=[
            questionary.Choice("Accept all", value="a"),
            questionary.Choice("Edit individually", value="e"),
            questionary.Choice("Skip all", value="s"),
        ],
    ).ask()
    if choice is None or choice == "s":
        return []

    if choice == "a":
        return [f.transformation for f in actionable]

    # Edit individually
    accepted = []
    for fix in actionable:
        import json as _json
        click.echo(f"\n  [{fix.code}] {fix.message}")
        click.echo(f"  Transform: {_json.dumps(fix.transformation)}")
        action = questionary.select(
            "Action:",
            choices=[
                questionary.Choice("Accept", value="accept"),
                questionary.Choice("Skip", value="skip"),
            ],
        ).ask()
        if action == "accept":
            accepted.append(fix.transformation)

    return accepted


def _build_evidence_blocks_interactive(source_name, preview_df, gene_column,
                                       skip_rows, evidence_categories, ai_suggestion=None):
    """Interactive wizard to build one or more evidence blocks.

    Uses preview DataFrame to show columns, prompts for gene/chr/pos mapping,
    then lets the user select evidence columns and configure each one.
    """
    import questionary
    import re
    from pegasus_v2f.integrate import suggest_mappings, validate_mapping, _NAME_CATEGORY_HINTS
    from rich.console import Console
    from rich.table import Table as RichTable

    console = Console()

    columns = _columns_from_preview(preview_df, skip_rows)
    suggestions = suggest_mappings(columns, source_name)
    col_names = [c["name"] for c in columns]

    # 1. Show columns
    col_table = RichTable(title="Detected Columns")
    col_table.add_column("Column", style="bold")
    col_table.add_column("Type")
    col_table.add_column("Sample Values")
    for c in columns:
        col_table.add_row(c["name"], c["type"], ", ".join(c["sample_values"][:3]))
    console.print(col_table)

    # 2. Pick gene column
    gene_default = suggestions["fields"].get("gene", gene_column)
    if gene_default not in col_names:
        gene_default = None
    gene_col = _pick_column("Gene column", col_names, default=gene_default)

    # 3. Variant toggle
    suggest_variant = suggestions["centric"] == "variant"
    is_variant = questionary.confirm(
        "Does this source include variant positions (chr/pos)?",
        default=suggest_variant,
    ).ask()
    if is_variant is None:
        raise SystemExit(0)

    used_cols = {gene_col}
    centric = "gene"
    chr_col = None
    pos_col = None
    rsid_col = None

    if is_variant:
        centric = "variant"
        # Pick chr column
        chr_default = suggestions["fields"].get("chromosome")
        if chr_default not in col_names:
            chr_default = None
        chr_col = _pick_column("Chromosome column", col_names, default=chr_default)
        used_cols.add(chr_col)

        # Pick pos column
        pos_default = suggestions["fields"].get("position")
        if pos_default not in col_names:
            pos_default = None
        pos_col = _pick_column("Position column", col_names, default=pos_default)
        used_cols.add(pos_col)

        # Optional rsid
        rsid_default = suggestions["fields"].get("rsid")
        if rsid_default in col_names:
            use_rsid = questionary.confirm(
                f"Use '{rsid_default}' as rsID column?", default=True
            ).ask()
            if use_rsid:
                rsid_col = rsid_default
                used_cols.add(rsid_col)

    # 4. Select evidence columns
    remaining = [c for c in col_names if c not in used_cols]
    if not remaining:
        click.echo("No remaining columns for evidence. Adding source without evidence.")
        return None

    selected = questionary.checkbox(
        "Select columns to tag as evidence:",
        choices=remaining,
    ).ask()
    if selected is None:
        raise SystemExit(0)
    if not selected:
        click.echo("No evidence columns selected.")
        return None

    # 5. Per-column config — show category descriptions from profiles
    from pegasus_v2f.pegasus_schema import EVIDENCE_CATEGORY_PROFILES
    cats_sorted = sorted(evidence_categories.keys())
    cat_choices = []
    for c in cats_sorted:
        profile = EVIDENCE_CATEGORY_PROFILES.get(c)
        desc = f" ({profile.description[:50]})" if profile else ""
        cat_choices.append(f"{c} — {evidence_categories[c]}{desc}")

    # Try to guess category from source name (or AI suggestion)
    suggested_cat = None
    if ai_suggestion and ai_suggestion.category:
        suggested_cat = ai_suggestion.category
    else:
        name_lower = source_name.lower()
        for hint, cat in _NAME_CATEGORY_HINTS.items():
            if hint in name_lower:
                suggested_cat = cat
                break

    ev_blocks = []
    for col in selected:
        click.echo(f"\n--- Configuring column: [bold]{col}[/bold] ---")

        # Category
        cat_default = None
        if suggested_cat:
            cat_default = next((ch for ch in cat_choices if ch.startswith(f"{suggested_cat} — ")), None)
        cat_answer = questionary.select(
            f"Evidence category for '{col}':",
            choices=cat_choices,
            default=cat_default,
        ).ask()
        if cat_answer is None:
            raise SystemExit(0)
        category = cat_answer.split(" — ")[0]

        # Field type
        col_info = next((c for c in columns if c["name"] == col), None)
        is_numeric = col_info and col_info["type"] == "numeric"
        field_type_default = "score" if is_numeric else "presence"
        field_type = questionary.select(
            f"What does '{col}' represent?",
            choices=["pvalue", "score", "effect_size", "presence"],
            default=field_type_default,
        ).ask()
        if field_type is None:
            raise SystemExit(0)

        # Source tag
        sanitized = re.sub(r"[^a-zA-Z0-9]", "_", col).strip("_").lower()
        default_tag = f"{source_name}_{sanitized}"
        tag = questionary.text(
            f"Source tag for '{col}':",
            default=default_tag,
        ).ask()
        if tag is None:
            raise SystemExit(0)

        # Build fields dict
        fields = {"gene": gene_col, field_type: col}
        if is_variant:
            fields["chromosome"] = chr_col
            fields["position"] = pos_col
            if rsid_col:
                fields["rsid"] = rsid_col

        ev_blocks.append({
            "source_tag": tag,
            "category": category,
            "centric": centric,
            "fields": fields,
        })

    # 6. Optional traits (shared)
    trait_input = questionary.text(
        "Trait tags (comma-separated, or Enter to skip):",
        default="",
    ).ask()
    if trait_input and trait_input.strip():
        trait_list = [t.strip() for t in trait_input.split(",") if t.strip()]
        for block in ev_blocks:
            block["traits"] = trait_list

    # 7. Summary + confirm
    click.echo()
    summary_table = RichTable(title="Evidence Blocks")
    summary_table.add_column("Source Tag", style="bold")
    summary_table.add_column("Category")
    summary_table.add_column("Centric")
    summary_table.add_column("Fields")
    for block in ev_blocks:
        fields_str = ", ".join(f"{k}={v}" for k, v in block["fields"].items())
        summary_table.add_row(
            block["source_tag"], block["category"], block["centric"], fields_str,
        )
    console.print(summary_table)

    # Validate each block
    all_errors = []
    for block in ev_blocks:
        errors = validate_mapping(block)
        all_errors.extend(errors)
    if all_errors:
        for err in all_errors:
            click.echo(f"  Validation error: {err}", err=True)
        raise click.ClickException("Evidence validation failed. Fix the issues and try again.")

    if not questionary.confirm("Proceed with these evidence blocks?", default=True).ask():
        raise SystemExit(0)

    return ev_blocks


@source.command("add")
@handle_errors
@click.argument("name")
@click.option("--type", "source_type", type=click.Choice(["googlesheets", "file", "excel", "url"]))
@click.option("--url", default=None, help="Source URL (Google Sheets or remote file).")
@click.option("--path", "file_path", default=None, help="Local file path.")
@click.option("--sheet", default=None, help="Sheet/tab name (for Google Sheets or Excel with multiple sheets).")
@click.option("--skip", "skip_rows", default=None, type=int, help="Rows to skip before header (skips preview).")
@click.option("--gene-column", default="gene", help="Column containing gene symbols.")
@click.option("--display-name", default=None, help="Human-readable display name.")
@click.option("--category", default=None, help="PEGASUS evidence category (e.g. QTL, COLOC, GWAS).")
@click.option("--traits", default=None, help="Comma-separated trait tags for this evidence.")
@click.option("--source-tag", default=None, help="Source tag identifier (defaults to NAME).")
@click.option("--centric", type=click.Choice(["gene", "variant"]), default=None, help="Evidence centric type.")
@click.option("--no-score", is_flag=True, help="Skip auto-scoring (for batch operations).")
@click.option("--force", is_flag=True, help="Replace source if it already exists.")
@click.option("--evidence-json", default=None,
    help="JSON list of evidence blocks (for multi-category non-interactive use).")
@click.option("--ai", "use_ai", is_flag=True, help="Enable AI-assisted category suggestion.")
@click.pass_context
def source_add(ctx, name, source_type, url, file_path, sheet, skip_rows, gene_column,
               display_name, category, traits, source_tag, centric, no_score, force,
               evidence_json, use_ai):
    """Add a data source with evidence configuration.

    Shows a preview of the first rows so you can confirm which row is the
    header. Then prompts for evidence category and trait tags (or use
    --category, --traits, --centric to skip prompts).
    """
    from pegasus_v2f.project import find_project_root
    from pegasus_v2f.config import read_config
    from pegasus_v2f.db import open_db
    from pegasus_v2f import sources as src_mod
    from pegasus_v2f.pegasus_schema import EVIDENCE_CATEGORIES

    source_def = {"name": name, "source_type": source_type or "file"}
    if url:
        source_def["url"] = url
    if file_path:
        source_def["path"] = file_path
    if sheet:
        source_def["sheet"] = sheet
    if gene_column != "gene":
        source_def["gene_column"] = gene_column
    if display_name:
        source_def["display_name"] = display_name

    db_arg = ctx.obj.get("db")
    config = None
    data_dir = None
    root = find_project_root(ctx.obj.get("project"))
    if root:
        config = read_config(root)
        data_dir = root / "data" / "raw"
        if not data_dir.exists():
            data_dir = root

    # Check for duplicates before downloading/previewing
    if not force:
        try:
            with open_db(db=db_arg, config=config, project_root=root) as conn:
                existing = src_mod.list_sources(conn)
                if any(s["name"] == name for s in existing):
                    raise click.ClickException(f"Source '{name}' already exists. Use --force to replace it.")
        except click.ClickException:
            raise
        except (FileNotFoundError, OSError):
            pass  # DB might not exist yet, that's fine

    # Preview: always show so the user can verify the header
    import pandas as pd
    from pegasus_v2f.loaders import preview_source
    from rich.console import Console
    from rich.table import Table as RichTable

    console = Console()
    is_gsheet = (source_def.get("source_type") == "googlesheets")

    # Fetch enough rows to show the header when skip is large
    n_preview = max(10, (skip_rows or 0) + 2)

    try:
        if is_gsheet:
            with console.status("Downloading spreadsheet..."):
                preview = preview_source(source_def, data_dir=data_dir, n_rows=n_preview)
        else:
            preview = preview_source(source_def, data_dir=data_dir, n_rows=n_preview)
    except Exception as e:
        raise click.ClickException(f"Could not fetch preview: {e}")

    table = RichTable(title=f"Preview: {name}", show_header=False)
    table.add_column("Row", style="dim", width=4)
    for col_idx in range(min(len(preview.columns), 6)):
        table.add_column(f"Col {col_idx}", overflow="ellipsis", max_width=30)

    for i, row in preview.iterrows():
        vals = [str(v) if pd.notna(v) else "" for v in row.values[:6]]
        style = "bold green" if skip_rows is not None and i == skip_rows else None
        table.add_row(str(i), *vals, style=style)

    console.print(table)

    if len(preview.columns) > 6:
        click.echo(f"  ... and {len(preview.columns) - 6} more columns")

    if skip_rows is None:
        skip_rows = click.prompt(
            "\nWhich row is the header? (rows above it will be skipped)",
            type=int,
            default=0,
        )
    elif category is None and evidence_json is None:
        # Only confirm interactively when not in non-interactive mode
        click.echo(f"\nHeader row: {skip_rows}")
        if not click.confirm("Proceed?", default=True):
            raise SystemExit(0)

    if skip_rows:
        source_def["skip_rows"] = skip_rows

    # --- Pre-wizard inspection + AI + fix suggestions ---
    ai_suggestion = None
    if category is None and evidence_json is None:
        # Interactive mode — run inspection before wizard
        from pegasus_v2f.loaders import load_source
        from pegasus_v2f.inspect import inspect_dataframe, render_inspection

        try:
            inspect_df = load_source(source_def, data_dir=data_dir)
            inspection = inspect_dataframe(inspect_df, source_name=name)
            render_inspection(inspection)

            # AI suggestion
            if use_ai:
                ai_suggestion = _show_ai_suggestion(inspection)

            # Interactive fix application
            if inspection.suggested_fixes:
                accepted_transforms = _apply_suggested_fixes(inspection.suggested_fixes)
                if accepted_transforms:
                    source_def.setdefault("transformations", []).extend(accepted_transforms)
        except Exception as e:
            click.echo(f"  Inspection skipped: {e}", err=True)

    # --- Evidence configuration ---
    if category is not None:
        # Non-interactive single-category (--category flag)
        trait_list = None
        if traits:
            trait_list = [t.strip() for t in traits.split(",") if t.strip()]
        ev_block = {
            "source_tag": source_tag or name,
            "category": category,
            "centric": centric or "gene",
            "fields": {"gene": gene_column},
        }
        if trait_list:
            ev_block["traits"] = trait_list
        source_def["evidence"] = [ev_block]
    elif evidence_json is not None:
        # Non-interactive multi-category (--evidence-json flag)
        import json as _json
        try:
            ev_blocks = _json.loads(evidence_json)
        except _json.JSONDecodeError as e:
            raise click.ClickException(f"Invalid --evidence-json: {e}")
        if not isinstance(ev_blocks, list) or not ev_blocks:
            raise click.ClickException("--evidence-json must be a non-empty JSON list")
        for block in ev_blocks:
            if "centric" not in block:
                fields = block.get("fields", {})
                block["centric"] = "variant" if ("chromosome" in fields and "position" in fields) else "gene"
        source_def["evidence"] = ev_blocks
    else:
        # Interactive wizard
        ev_blocks = _build_evidence_blocks_interactive(name, preview, gene_column,
                                                       skip_rows or 0, EVIDENCE_CATEGORIES,
                                                       ai_suggestion=ai_suggestion)
        if ev_blocks:
            source_def["evidence"] = ev_blocks

    from pegasus_v2f.report import Report, render_report

    report = Report(operation="source_add")

    try:
        with open_db(db=db_arg, config=config, project_root=root) as conn:
            if force:
                try:
                    src_mod.remove_source(conn, name, config=config)
                except ValueError:
                    pass  # Source didn't exist, that's fine
            rows = src_mod.add_source(conn, source_def, data_dir=data_dir, config=config,
                                      no_score=no_score, report=report)
    except (ValueError, FileNotFoundError) as e:
        raise click.ClickException(str(e))

    # Write source to v2f.yaml so file config stays in sync with DB
    if root:
        from pegasus_v2f.config import append_source_to_yaml, remove_source_from_yaml
        config_path = root / "v2f.yaml"
        if config_path.exists():
            if force:
                remove_source_from_yaml(config_path, name)
            append_source_to_yaml(config_path, source_def)

    click.echo(f"Added source '{name}': {rows} rows")
    if ctx.obj.get("json_output"):
        render_report(report, json_mode=True)
    elif report.has_warnings:
        render_report(report)


@source.command("inspect")
@handle_errors
@click.argument("name")
@click.option("--type", "source_type", type=click.Choice(["googlesheets", "file", "excel", "url"]))
@click.option("--url", default=None, help="Source URL.")
@click.option("--path", "file_path", default=None, help="Local file path.")
@click.option("--sheet", default=None, help="Sheet/tab name.")
@click.option("--skip", "skip_rows", default=None, type=int, help="Rows to skip before header.")
@click.option("--ai", "use_ai", is_flag=True, help="Enable AI-assisted category suggestion.")
@click.option("--json", "json_output", is_flag=True, help="Output structured JSON.")
@click.pass_context
def source_inspect(ctx, name, source_type, url, file_path, sheet, skip_rows, use_ai, json_output):
    """Inspect a data source for PEGASUS compatibility before loading.

    Profiles columns, validates gene symbols, checks chromosome formats,
    and suggests evidence category mappings. Use --ai for AI-assisted
    category detection via claude CLI.
    """
    from pegasus_v2f.project import find_project_root
    from pegasus_v2f.config import read_config
    from pegasus_v2f.loaders import load_source
    from pegasus_v2f.inspect import inspect_dataframe, render_inspection

    source_def = {"name": name, "source_type": source_type or "file"}
    if url:
        source_def["url"] = url
    if file_path:
        source_def["path"] = file_path
    if sheet:
        source_def["sheet"] = sheet
    if skip_rows:
        source_def["skip_rows"] = skip_rows

    root = find_project_root(ctx.obj.get("project"))
    data_dir = None
    if root:
        data_dir = root / "data" / "raw"
        if not data_dir.exists():
            data_dir = root

    try:
        df = load_source(source_def, data_dir=data_dir)
    except Exception as e:
        raise click.ClickException(f"Could not load source: {e}")

    result = inspect_dataframe(df, source_name=name)

    if json_output or ctx.obj.get("json_output"):
        import json as _json
        output = result.to_dict()
        # AI section added below if applicable
        if use_ai:
            from pegasus_v2f.ai_assist import get_provider
            provider = get_provider("auto")
            if provider:
                suggestion = provider.suggest(result)
                if suggestion:
                    output["ai_suggestion"] = suggestion.to_dict()
                else:
                    click.echo("AI suggestion failed — showing inspection only.", err=True)
            else:
                click.echo("AI provider not available (claude CLI not found).", err=True)
        print(_json.dumps(output, indent=2))
        return

    render_inspection(result)

    if use_ai:
        _show_ai_suggestion(result)


def _show_ai_suggestion(result):
    """Run AI provider and display suggestion. Gracefully degrades on failure."""
    from pegasus_v2f.ai_assist import get_provider
    from pegasus_v2f.pegasus_schema import EVIDENCE_CATEGORIES
    from rich.console import Console
    import sys

    console = Console(stderr=True, file=sys.stderr)
    provider = get_provider("auto")
    if not provider:
        console.print("  [yellow]AI provider not available (claude CLI not found)[/yellow]\n")
        return None

    console.print(f"\n  [dim]Running AI analysis ({provider.name})...[/dim]")
    suggestion = provider.suggest(result)
    if not suggestion:
        console.print("  [yellow]AI suggestion failed — continuing without AI[/yellow]\n")
        return None

    console.print(f"\n  [bold]AI Analysis[/bold] ({provider.name})")
    if suggestion.category:
        label = EVIDENCE_CATEGORIES.get(suggestion.category, "")
        console.print(f"    Category: [bold]{suggestion.category}[/bold] ({label})")
    console.print(f"    \"{suggestion.category_reasoning}\"")
    if suggestion.column_mappings:
        mappings_str = ", ".join(f"{k}->{v}" for k, v in suggestion.column_mappings.items())
        console.print(f"    Mappings: {mappings_str}")
    console.print(f"    Centric: {suggestion.centric}")
    if suggestion.quality_notes:
        console.print(f"\n    Notes:")
        for note in suggestion.quality_notes:
            console.print(f"    - {note}")
    console.print(f"    Confidence: {suggestion.confidence:.0%}\n")

    return suggestion


@source.command("show")
@handle_errors
@click.argument("name")
@click.pass_context
def source_show(ctx, name):
    """Show details for a single source."""
    from pegasus_v2f.project import find_project_root
    from pegasus_v2f.config import read_config, get_data_sources
    from pegasus_v2f.db import open_db, is_postgres
    from rich.console import Console

    root = find_project_root(ctx.obj.get("project"))
    if not root:
        raise click.ClickException("Not in a v2f project (no v2f.yaml found)")

    config = read_config(root)
    sources = get_data_sources(config)
    match = [s for s in sources if s["name"] == name]
    if not match:
        raise click.ClickException(f"Source '{name}' not found in config")
    src = match[0]

    console = Console()
    console.print(f"[bold]Name:[/bold] {src['name']}")
    console.print(f"[bold]Type:[/bold] {src.get('source_type', 'file')}")
    if src.get("display_name"):
        console.print(f"[bold]Display Name:[/bold] {src['display_name']}")
    if src.get("url"):
        console.print(f"[bold]URL:[/bold] {src['url']}")
    if src.get("path"):
        console.print(f"[bold]Path:[/bold] {src['path']}")
    if src.get("sheet"):
        console.print(f"[bold]Sheet:[/bold] {src['sheet']}")
    if src.get("gene_column") and src["gene_column"] != "gene":
        console.print(f"[bold]Gene Column:[/bold] {src['gene_column']}")
    if src.get("evidence"):
        ev = src["evidence"]
        if ev.get("role"):
            console.print(f"[bold]Role:[/bold] {ev['role']}")
        if ev.get("category"):
            console.print(f"[bold]Category:[/bold] {ev['category']} ({ev.get('centric', '')})")

    # Try to get row count and columns from DB
    from pegasus_v2f.db import raw_table_name
    raw_name = raw_table_name(name)
    db_arg = ctx.obj.get("db")
    try:
        with open_db(db=db_arg, config=config, read_only=True, project_root=root) as conn:
            if is_postgres(conn):
                cur = conn.cursor()
                cur.execute(f'SELECT COUNT(*) FROM "{raw_name}"')
                row_count = cur.fetchone()[0]
                cur.execute(f'SELECT * FROM "{raw_name}" LIMIT 0')
                columns = [desc[0] for desc in cur.description]
                cur.close()
            else:
                row_count = conn.execute(f'SELECT COUNT(*) FROM "{raw_name}"').fetchone()[0]
                columns = [desc[0] for desc in conn.execute(f'SELECT * FROM "{raw_name}" LIMIT 0').description]
            console.print(f"[bold]Rows:[/bold] {row_count:,}")
            console.print(f"[bold]Columns:[/bold] {', '.join(columns)}")
    except Exception:
        pass  # Table might not be loaded yet


@source.command("update")
@handle_errors
@click.argument("name")
@click.pass_context
def source_update(ctx, name):
    """Re-fetch and reload a source."""
    from pegasus_v2f.project import find_project_root
    from pegasus_v2f.config import read_config
    from pegasus_v2f.db import open_db
    from pegasus_v2f import sources as src_mod

    db_arg = ctx.obj.get("db")
    config = None
    data_dir = None
    root = find_project_root(ctx.obj.get("project"))
    if root:
        config = read_config(root)
        data_dir = root / "data" / "raw"
        if not data_dir.exists():
            data_dir = root

    try:
        with open_db(db=db_arg, config=config, project_root=root) as conn:
            rows = src_mod.update_source(conn, name, data_dir=data_dir)
    except ValueError as e:
        raise click.ClickException(str(e))

    click.echo(f"Updated source '{name}': {rows} rows")


@source.command("remove")
@handle_errors
@click.argument("name")
@click.pass_context
def source_remove(ctx, name):
    """Drop table and remove source from config."""
    from pegasus_v2f.project import find_project_root
    from pegasus_v2f.config import read_config
    from pegasus_v2f.db import open_db
    from pegasus_v2f import sources as src_mod

    if not click.confirm(f"Remove source '{name}'?"):
        click.echo("Cancelled.")
        return

    db_arg = ctx.obj.get("db")
    config = None
    root = find_project_root(ctx.obj.get("project"))
    if root:
        config = read_config(root)

    try:
        with open_db(db=db_arg, config=config, project_root=root) as conn:
            src_mod.remove_source(conn, name, config=config)
    except ValueError as e:
        raise click.ClickException(str(e))

    # Remove from v2f.yaml too
    if root:
        from pegasus_v2f.config import remove_source_from_yaml
        config_path = root / "v2f.yaml"
        if config_path.exists():
            remove_source_from_yaml(config_path, name)

    click.echo(f"Removed source '{name}'")


    # NOTE: `v2f source integrate` has been removed. Evidence integration now
    # happens during `v2f source add` — sources with `evidence:` blocks in their
    # config are automatically loaded into the unified evidence table.

# =====================================================================
# Study group
# =====================================================================

class StudyGroup(click.RichGroup):
    """Custom group that handles fixed commands (list, add, show, set, remove)
    and dynamic id_prefix dispatch for trait management.

    Any unrecognized subcommand is treated as a study id_prefix, giving access
    to per-study trait commands.
    """

    def get_command(self, ctx, cmd_name):
        # Try fixed subcommands first (list, add, show, set, remove)
        rv = super().get_command(ctx, cmd_name)
        if rv is not None:
            return rv
        # Treat anything else as a study id_prefix for trait access
        return _make_study_id_group(cmd_name)

    def list_commands(self, ctx):
        return sorted(super().list_commands(ctx))

    def format_epilog(self, ctx, formatter):
        from rich.table import Table
        from rich_click.rich_help_rendering import RichClickRichPanel

        table = Table(highlight=False, box=None, show_header=False, pad_edge=False)
        table.add_column("command", style="bold")
        table.add_column("description")
        for cmd, desc in [
            ("<id> trait list", "List traits"),
            ("<id> trait add <trait>", "Add a trait"),
            ("<id> trait remove <trait>", "Remove a trait"),
            ("<id> trait set <trait> <key> <value>", "Set trait metadata"),
        ]:
            table.add_row(cmd, desc)

        panel = RichClickRichPanel(table, title="Per-Study Trait Commands", border_style="dim")
        formatter.write(panel)


def _make_study_id_group(id_prefix: str) -> click.Group:
    """Create a dynamic Click group for a specific study id_prefix (trait access only)."""

    @click.group(name=id_prefix, invoke_without_command=True)
    @click.pass_context
    def study_id_group(ctx):
        """Trait commands for a specific study."""
        ctx.ensure_object(dict)
        ctx.obj["study_id_prefix"] = id_prefix
        if ctx.invoked_subcommand is None:
            click.echo(ctx.get_help())

    # --- Nested trait group ---
    @study_id_group.group("trait")
    @click.pass_context
    def trait_group(ctx):
        """Manage traits for this study."""
        ctx.ensure_object(dict)
        ctx.obj["study_id_prefix"] = id_prefix

    @trait_group.command("list")
    @handle_errors
    @click.pass_context
    def trait_list(ctx):
        """List traits for this study."""
        from pegasus_v2f.project import find_project_root
        from pegasus_v2f.config import read_config, get_study_by_id

        root = find_project_root(ctx.obj.get("project"))
        if not root:
            raise click.ClickException("Not in a v2f project (no v2f.yaml found)")
        config = read_config(root)
        study = get_study_by_id(config, id_prefix)
        if not study:
            raise click.ClickException(f"Study '{id_prefix}' not found")
        for t in study.get("traits", []):
            click.echo(t)

    @trait_group.command("add")
    @handle_errors
    @click.argument("trait")
    @click.pass_context
    def trait_add(ctx, trait):
        """Add a trait to this study."""
        from pegasus_v2f.project import find_project_root
        from pegasus_v2f.config import add_trait_to_study

        root = find_project_root(ctx.obj.get("project"))
        if not root:
            raise click.ClickException("Not in a v2f project (no v2f.yaml found)")
        config_path = root / "v2f.yaml"
        try:
            add_trait_to_study(config_path, id_prefix, trait)
        except ValueError as e:
            raise click.ClickException(str(e))
        click.echo(f"Added trait '{trait}' to study '{id_prefix}'")

    @trait_group.command("remove")
    @handle_errors
    @click.argument("trait")
    @click.pass_context
    def trait_remove(ctx, trait):
        """Remove a trait from this study."""
        from pegasus_v2f.project import find_project_root
        from pegasus_v2f.config import remove_trait_from_study

        root = find_project_root(ctx.obj.get("project"))
        if not root:
            raise click.ClickException("Not in a v2f project (no v2f.yaml found)")
        config_path = root / "v2f.yaml"
        try:
            remove_trait_from_study(config_path, id_prefix, trait)
        except ValueError as e:
            raise click.ClickException(str(e))
        click.echo(f"Removed trait '{trait}' from study '{id_prefix}'")

    @trait_group.command("set")
    @handle_errors
    @click.argument("trait")
    @click.argument("key")
    @click.argument("value")
    @click.pass_context
    def trait_set(ctx, trait, key, value):
        """Set a trait-level field (description, ontology_id)."""
        from pegasus_v2f.project import find_project_root
        from pegasus_v2f.config import read_config, get_study_by_id, update_study_in_yaml

        allowed_keys = {"description": "trait_descriptions", "ontology_id": "trait_ontology_ids"}
        if key not in allowed_keys:
            raise click.ClickException(f"Unknown trait key '{key}'. Allowed: {', '.join(allowed_keys)}")

        root = find_project_root(ctx.obj.get("project"))
        if not root:
            raise click.ClickException("Not in a v2f project (no v2f.yaml found)")

        config = read_config(root)
        study = get_study_by_id(config, id_prefix)
        if not study:
            raise click.ClickException(f"Study '{id_prefix}' not found")
        if trait not in study.get("traits", []):
            raise click.ClickException(f"Trait '{trait}' not found in study '{id_prefix}'")

        yaml_key = allowed_keys[key]
        current = dict(study.get(yaml_key, {}))
        current[trait] = value

        config_path = root / "v2f.yaml"
        try:
            update_study_in_yaml(config_path, id_prefix, yaml_key, current)
        except ValueError as e:
            raise click.ClickException(str(e))

        click.echo(f"Set {key}='{value}' for trait '{trait}' in study '{id_prefix}'")

    return study_id_group


@cli.group(cls=StudyGroup)
@click.pass_context
def study(ctx):
    """Manage PEGASUS studies."""
    pass


@study.command("list")
@handle_errors
@click.pass_context
def study_list(ctx):
    """List all configured studies."""
    from pegasus_v2f.project import find_project_root
    from pegasus_v2f.config import read_config, get_study_list
    from rich.console import Console
    from rich.table import Table as RichTable

    root = find_project_root(ctx.obj.get("project"))
    if not root:
        raise click.ClickException("Not in a v2f project (no v2f.yaml found)")

    config = read_config(root)
    studies = get_study_list(config)

    if not studies:
        click.echo("No studies configured. Use 'v2f study add' to create one.")
        return

    console = Console()
    table = RichTable(title="Studies")
    table.add_column("ID Prefix", style="bold")
    table.add_column("Traits")
    table.add_column("Build")
    table.add_column("GWAS Source")
    table.add_column("Ancestry")
    table.add_column("Sex")
    table.add_column("Year")
    table.add_column("DOI")

    for s in studies:
        table.add_row(
            s.get("id_prefix", ""),
            ", ".join(s.get("traits", [])),
            s.get("genome_build", ""),
            s.get("gwas_source", ""),
            s.get("ancestry", ""),
            s.get("sex", ""),
            str(s.get("year", "")),
            s.get("doi", ""),
        )

    console.print(table)


@study.command("add")
@handle_errors
@click.argument("study_name", required=False)
@click.option("--loci", "loci_file", default=None, help="Sentinel variant file (local path or Google Sheets URL).")
@click.option("--sheet", "loci_sheet", default=None, help="Sheet/tab name (for Excel or Google Sheets with multiple tabs).")
@click.option("--skip", "loci_skip", default=None, type=int, help="Rows to skip before header in loci file.")
@click.option("--gene-column", default=None, help="Column with nearest gene per sentinel (stored on loci, not scored).")
@click.option("--sentinel-column", default=None, help="Column with variant ID (chr_pos_ref_alt format, any separator).")
@click.option("--pvalue-column", default=None, help="Column with p-value per sentinel.")
@click.option("--rsid-column", default=None, help="Column with rsID per sentinel.")
@click.option("--traits", "traits_str", help="Comma-separated trait names")
@click.option("--gwas-source", help="GWAS source (PMID or GCST)")
@click.option("--ancestry", help="Population ancestry")
@click.option("--sex", help="Sex restriction (male/female/both)")
@click.option("--sample-size", type=int, help="GWAS sample size")
@click.option("--doi", help="Publication DOI")
@click.option("--year", type=int, help="Publication year")
@click.option("--window-kb", type=int, default=500, help="Locus window half-size in kb (default: 500)")
@click.option("--merge-kb", type=int, default=250, help="Merge distance in kb (default: 250)")
@click.option("--force", is_flag=True, help="Replace study if it already exists.")
@click.pass_context
def study_add(ctx, study_name, loci_file, loci_sheet, loci_skip, gene_column,
              sentinel_column, pvalue_column, rsid_column,
              traits_str, gwas_source, ancestry,
              sex, sample_size, doi, year, window_kb, merge_kb, force):
    """Add a new study with loci from a sentinel variant file.

    If STUDY_NAME, --loci, and --traits are provided, runs non-interactively.
    Otherwise, prompts for missing values. --loci accepts a local file path
    or a Google Sheets URL.
    """
    from pathlib import Path
    from pegasus_v2f.project import find_project_root
    from pegasus_v2f.config import read_config, add_study_to_yaml
    from pegasus_v2f.db import get_connection

    root = find_project_root(ctx.obj.get("project"))
    if not root:
        raise click.ClickException("Not in a v2f project (no v2f.yaml found)")

    config_path = root / "v2f.yaml"

    import pandas as pd
    from rich.console import Console
    from rich.table import Table as RichTable
    console = Console()

    # Interactive: prompt for study name first (needed for context)
    interactive = not study_name or not traits_str
    if interactive:
        import questionary
        if not study_name:
            study_name = questionary.text("Study name (e.g. shrine_2023):").ask()
            if study_name is None:
                raise SystemExit(0)
        if not loci_file:
            loci_path_input = questionary.path("Sentinel variant file (local path or Google Sheets URL):").ask()
            if loci_path_input is None:
                raise SystemExit(0)
            loci_file = loci_path_input

    # --- Check for existing study (before downloading anything) ---
    config = read_config(root)
    db_arg = ctx.obj.get("db")
    conn = get_connection(db=db_arg, config=config, project_root=root)
    try:
        from pegasus_v2f.pegasus_schema import create_pegasus_schema
        create_pegasus_schema(conn)
        existing = conn.execute(
            "SELECT COUNT(*) FROM studies WHERE study_name = ?", [study_name]
        ).fetchone()[0]
    finally:
        conn.close()

    if existing > 0:
        if force:
            from pegasus_v2f.study_management import remove_study as _remove_study
            conn = get_connection(db=db_arg, config=config, project_root=root)
            try:
                _remove_study(conn, study_name)
            finally:
                conn.close()
            from pegasus_v2f.config import remove_study_from_yaml
            try:
                remove_study_from_yaml(config_path, study_name)
            except ValueError:
                pass
            click.echo(f"Removed existing study '{study_name}'")
        else:
            raise click.ClickException(
                f"Study '{study_name}' already exists. Use --force to replace it."
            )

    # --- Loci preview + loading (before remaining prompts) ---
    loci_df = None
    loci_path = None
    if loci_file:
        if loci_file.startswith("http"):
            from pegasus_v2f.loaders import load_googlesheets, preview_source
            source_spec = {"url": loci_file, "source_type": "googlesheets"}
            if loci_sheet:
                source_spec["sheet"] = loci_sheet

            n_preview = max(10, (loci_skip or 0) + 2)
            try:
                with console.status("Downloading spreadsheet..."):
                    preview = preview_source(source_spec, n_rows=n_preview)
            except Exception as e:
                raise click.ClickException(f"Could not fetch preview: {e}")

            table = RichTable(title=f"Loci preview: {loci_sheet or 'default sheet'}", show_header=False)
            table.add_column("Row", style="dim", width=4)
            for col_idx in range(min(len(preview.columns), 6)):
                table.add_column(f"Col {col_idx}", overflow="ellipsis", max_width=30)
            for i, row in preview.iterrows():
                vals = [str(v) if pd.notna(v) else "" for v in row.values[:6]]
                style = "bold green" if loci_skip is not None and i == loci_skip else None
                table.add_row(str(i), *vals, style=style)
            console.print(table)
            if len(preview.columns) > 6:
                click.echo(f"  ... and {len(preview.columns) - 6} more columns")

            if loci_skip is None:
                loci_skip = click.prompt(
                    "\nWhich row is the header? (rows above it will be skipped)",
                    type=int, default=0,
                )
            else:
                click.echo(f"\nHeader row: {loci_skip}")
                if not click.confirm("Proceed?", default=True):
                    raise SystemExit(0)

            if loci_skip:
                source_spec["skip_rows"] = loci_skip
            loci_df = load_googlesheets(source_spec)
        else:
            loci_path = Path(loci_file)
            if not loci_path.exists():
                raise click.ClickException(f"Loci file not found: {loci_path}")

            if loci_path.suffix.lower() in (".xlsx", ".xls"):
                from pegasus_v2f.loaders import preview_source
                source_spec = {"path": str(loci_path), "source_type": "excel"}
                if loci_sheet:
                    source_spec["sheet"] = loci_sheet

                n_preview = max(10, (loci_skip or 0) + 2)
                try:
                    preview = preview_source(source_spec, n_rows=n_preview)
                except Exception as e:
                    raise click.ClickException(f"Could not fetch preview: {e}")

                table = RichTable(title=f"Loci preview: {loci_path.name}", show_header=False)
                table.add_column("Row", style="dim", width=4)
                for col_idx in range(min(len(preview.columns), 6)):
                    table.add_column(f"Col {col_idx}", overflow="ellipsis", max_width=30)
                for i, row in preview.iterrows():
                    vals = [str(v) if pd.notna(v) else "" for v in row.values[:6]]
                    style = "bold green" if loci_skip is not None and i == loci_skip else None
                    table.add_row(str(i), *vals, style=style)
                console.print(table)
                if len(preview.columns) > 6:
                    click.echo(f"  ... and {len(preview.columns) - 6} more columns")

                if loci_skip is None:
                    loci_skip = click.prompt(
                        "\nWhich row is the header? (rows above it will be skipped)",
                        type=int, default=0,
                    )
                else:
                    click.echo(f"\nHeader row: {loci_skip}")
                    if not click.confirm("Proceed?", default=True):
                        raise SystemExit(0)

                kwargs = {"engine": "calamine"}
                if loci_sheet:
                    kwargs["sheet_name"] = loci_sheet
                if loci_skip:
                    kwargs["skiprows"] = loci_skip
                loci_df = pd.read_excel(loci_path, **kwargs)
                loci_path = None  # use loci_df instead

    # --- Remaining interactive prompts (after preview) ---
    if interactive:
        if gene_column is None and loci_file:
            # Offer gene column selection from available columns
            available_cols = []
            if loci_df is not None:
                available_cols = [str(c) for c in loci_df.columns]
            elif loci_path is not None:
                # Peek at header for TSV/CSV files
                try:
                    peek = pd.read_csv(loci_path, nrows=0, sep="\t" if str(loci_path).endswith((".tsv", ".gz")) else ",")
                    available_cols = [str(c) for c in peek.columns]
                except Exception:
                    pass
            if available_cols:
                col_choices = ["(none)"] + available_cols

                if gene_column is None:
                    gene_answer = questionary.select(
                        "Nearest gene column (stored on loci, not scored):",
                        choices=col_choices,
                        default="(none)",
                    ).ask()
                    if gene_answer is None:
                        raise SystemExit(0)
                    if gene_answer != "(none)":
                        gene_column = gene_answer

                if sentinel_column is None:
                    sent_answer = questionary.select(
                        "Sentinel variant ID column (chr_pos_ref_alt format):",
                        choices=col_choices,
                        default="(none)",
                    ).ask()
                    if sent_answer is None:
                        raise SystemExit(0)
                    if sent_answer != "(none)":
                        sentinel_column = sent_answer

                if pvalue_column is None:
                    pval_answer = questionary.select(
                        "P-value column:",
                        choices=col_choices,
                        default="(none)",
                    ).ask()
                    if pval_answer is None:
                        raise SystemExit(0)
                    if pval_answer != "(none)":
                        pvalue_column = pval_answer

                if rsid_column is None:
                    rsid_answer = questionary.select(
                        "rsID column:",
                        choices=col_choices,
                        default="(none)",
                    ).ask()
                    if rsid_answer is None:
                        raise SystemExit(0)
                    if rsid_answer != "(none)":
                        rsid_column = rsid_answer
        if not traits_str:
            traits_str = questionary.text("Traits (comma-separated, e.g. FEV1,FVC):").ask()
            if traits_str is None:
                raise SystemExit(0)
        if not gwas_source:
            gwas_source = questionary.text("GWAS source (PMID or GCST, optional):").ask() or None
        if not ancestry:
            ancestry = questionary.text("Ancestry (optional):").ask() or None
        if not sample_size:
            size_str = questionary.text("Sample size (optional):").ask()
            if size_str:
                sample_size = int(size_str)
        if not doi:
            doi = questionary.text("DOI (optional):").ask() or None
        if not year:
            year_str = questionary.text("Year (optional):").ask()
            if year_str:
                year = int(year_str)
        if window_kb == 500:
            wk_str = questionary.text("Locus window half-size in kb:", default="500").ask()
            if wk_str:
                window_kb = int(wk_str)
        if merge_kb == 250:
            mk_str = questionary.text("Merge distance in kb:", default="250").ask()
            if mk_str:
                merge_kb = int(mk_str)

    traits = [t.strip() for t in traits_str.split(",") if t.strip()]
    if not traits:
        raise click.ClickException("At least one trait is required")

    genome_build = config.get("database", {}).get("genome_build", "hg38")

    if interactive:
        build_answer = questionary.text("Genome build:", default=genome_build).ask()
        if build_answer is None:
            raise SystemExit(0)
        genome_build = build_answer.strip() or genome_build

    # If loci provided, create studies and loci in DB
    if loci_file:
        from pegasus_v2f.study_management import add_study as _add_study
        from pegasus_v2f.report import Report, render_report

        report = Report(operation="study_add")

        conn = get_connection(db=db_arg, config=config, project_root=root)
        try:
            create_pegasus_schema(conn)
            result = _add_study(
                conn,
                study_name=study_name,
                traits=traits,
                loci_file=loci_path,
                loci_df=loci_df,
                gwas_source=gwas_source,
                ancestry=ancestry,
                sex=sex,
                sample_size=sample_size,
                doi=doi,
                year=year,
                genome_build=genome_build,
                gene_column=gene_column,
                sentinel_column=sentinel_column,
                pvalue_column=pvalue_column,
                rsid_column=rsid_column,
                loci_source=loci_file,
                loci_sheet=loci_sheet,
                loci_skip=loci_skip,
                window_kb=window_kb,
                merge_distance_kb=merge_kb,
                cache_dir=root / ".v2f",
                config_path=config_path,
                report=report,
            )
            click.echo(
                f"Added study '{study_name}' with {len(traits)} trait(s): "
                f"{result['n_loci']} loci from {result['n_sentinels']} sentinels"
            )
            if ctx.obj.get("json_output"):
                render_report(report, json_mode=True)
            elif report.has_warnings:
                render_report(report)
        finally:
            conn.close()
    else:
        # No loci file — just write config (study_management won't be called)
        study_config = {"id_prefix": study_name, "traits": traits}
        if gwas_source:
            study_config["gwas_source"] = gwas_source
        if ancestry:
            study_config["ancestry"] = ancestry
        if doi:
            study_config["doi"] = doi
        if year:
            study_config["year"] = year

        locus_config = None
        if not config.get("pegasus", {}).get("locus_definition"):
            locus_config = {"window_kb": window_kb, "merge_distance_kb": merge_kb}

        try:
            add_study_to_yaml(config_path, study_config, locus_config)
        except ValueError as e:
            raise click.ClickException(str(e))

        click.echo(f"Added study '{study_name}' with traits: {', '.join(traits)} (no loci file — use --loci to create loci)")


@study.command("show")
@handle_errors
@click.argument("id_prefix")
@click.pass_context
def study_show(ctx, id_prefix):
    """Show details for a study."""
    from pegasus_v2f.project import find_project_root
    from pegasus_v2f.config import read_config, get_study_by_id
    from rich.console import Console

    root = find_project_root(ctx.obj.get("project"))
    if not root:
        raise click.ClickException("Not in a v2f project (no v2f.yaml found)")
    config = read_config(root)
    study = get_study_by_id(config, id_prefix)
    if not study:
        raise click.ClickException(f"Study '{id_prefix}' not found")

    console = Console()
    console.print(f"[bold]Study:[/bold] {study['id_prefix']}")
    console.print(f"[bold]Traits:[/bold] {', '.join(study.get('traits', []))}")
    for key in ("genome_build", "gwas_source", "ancestry", "study_description",
                 "sample_size", "doi", "year"):
        if study.get(key):
            console.print(f"[bold]{key}:[/bold] {study[key]}")
    if study.get("trait_descriptions"):
        console.print("[bold]trait_descriptions:[/bold]")
        for t, d in study["trait_descriptions"].items():
            console.print(f"  {t}: {d}")
    if study.get("trait_ontology_ids"):
        console.print("[bold]trait_ontology_ids:[/bold]")
        for t, o in study["trait_ontology_ids"].items():
            console.print(f"  {t}: {o}")

    # Show DB-level info if available
    db_arg = ctx.obj.get("db")
    try:
        from pegasus_v2f.db import open_db
        with open_db(db=db_arg, config=config, read_only=True, project_root=root) as conn:
            # Loci count per trait
            rows = conn.execute(
                "SELECT s.trait, COUNT(l.locus_id) FROM studies s "
                "LEFT JOIN loci l ON s.study_id = l.study_id "
                "WHERE s.study_name = ? GROUP BY s.trait ORDER BY s.trait",
                [id_prefix],
            ).fetchall()
            if rows:
                console.print("[bold]Loci per trait:[/bold]")
                for trait, count in rows:
                    console.print(f"  {trait}: {count}")

            # Provenance table
            raw_table = f"loci_{id_prefix}"
            try:
                n = conn.execute(f'SELECT COUNT(*) FROM "{raw_table}"').fetchone()[0]
                console.print(f"[bold]Provenance:[/bold] {raw_table} ({n} sentinels)")
            except Exception:
                pass
    except Exception:
        pass


@study.command("preview")
@handle_errors
@click.argument("study_name")
@click.option("--detail", is_flag=True, help="Show per-gene evidence breakdown")
@click.pass_context
def study_preview(ctx, study_name, detail):
    """Preview scoring for a study — show candidate genes and evidence per locus."""
    from pegasus_v2f.db import get_connection
    from pegasus_v2f.study_management import preview_study
    from rich.console import Console
    from rich.table import Table as RichTable

    from pegasus_v2f.project import find_project_root
    from pegasus_v2f.config import read_config
    from pegasus_v2f.pegasus_schema import create_pegasus_schema

    db_arg = ctx.obj.get("db")
    root = find_project_root(ctx.obj.get("project"))
    config = read_config(root) if root else None

    if not db_arg and not root:
        raise click.ClickException("No database found. Provide --db or run from a v2f project.")

    conn = get_connection(db=db_arg, config=config, project_root=root)
    try:
        create_pegasus_schema(conn)
        results = preview_study(conn, study_name)
        if not results:
            click.echo(f"No loci found for study '{study_name}'")
            return

        console = Console()
        table = RichTable(title=f"Preview: {study_name}")
        table.add_column("Locus", style="bold")
        table.add_column("Chr")
        table.add_column("Start")
        table.add_column("End")
        table.add_column("Candidates", justify="right")
        table.add_column("Evidence", justify="right")
        table.add_column("Categories")

        for r in results:
            cats = ", ".join(f"{k}:{v}" for k, v in sorted(r["evidence_by_category"].items()))
            table.add_row(
                r["locus_id"],
                r["chromosome"],
                str(r["start_position"]),
                str(r["end_position"]),
                str(r["n_candidate_genes"]),
                str(r["n_evidence_rows"]),
                cats or "-",
            )

        console.print(table)
        click.echo(f"\n{len(results)} loci, "
                    f"{sum(r['n_candidate_genes'] for r in results)} total candidate genes, "
                    f"{sum(r['n_evidence_rows'] for r in results)} total evidence rows")
    finally:
        conn.close()


@study.command("set")
@handle_errors
@click.argument("id_prefix")
@click.argument("key")
@click.argument("value")
@click.pass_context
def study_set(ctx, id_prefix, key, value):
    """Update a study field (gwas_source, ancestry, genome_build, study_description, trait_descriptions, trait_ontology_ids, sample_size, doi, year)."""
    from pegasus_v2f.project import find_project_root
    from pegasus_v2f.config import update_study_in_yaml

    root = find_project_root(ctx.obj.get("project"))
    if not root:
        raise click.ClickException("Not in a v2f project (no v2f.yaml found)")
    config_path = root / "v2f.yaml"
    try:
        update_study_in_yaml(config_path, id_prefix, key, value)
    except ValueError as e:
        raise click.ClickException(str(e))
    click.echo(f"Updated {id_prefix}.{key} = {value}")


@study.command("remove")
@handle_errors
@click.argument("id_prefix")
@click.pass_context
def study_remove(ctx, id_prefix):
    """Remove a study from config."""
    from pegasus_v2f.project import find_project_root
    from pegasus_v2f.config import read_config, get_data_sources, remove_study_from_yaml

    root = find_project_root(ctx.obj.get("project"))
    if not root:
        raise click.ClickException("Not in a v2f project (no v2f.yaml found)")

    config = read_config(root)
    # Warn about linked sources
    linked = [
        s["name"] for s in get_data_sources(config)
        if s.get("evidence", {}).get("study") == id_prefix
    ]
    if linked:
        click.echo(f"Warning: {len(linked)} data source(s) reference this study: {', '.join(linked)}")

    if not click.confirm(f"Remove study '{id_prefix}'?"):
        click.echo("Cancelled.")
        return

    config_path = root / "v2f.yaml"
    try:
        remove_study_from_yaml(config_path, id_prefix)
    except ValueError as e:
        raise click.ClickException(str(e))
    click.echo(f"Removed study '{id_prefix}'")


# =====================================================================
# Table group
# =====================================================================

@cli.group()
@click.pass_context
def table(ctx):
    """Inspect database tables."""
    pass


@table.command("list")
@handle_errors
@click.pass_context
def table_list(ctx):
    """List tables with row counts."""
    from pegasus_v2f.project import find_project_root
    from pegasus_v2f.config import read_config
    from pegasus_v2f.db import open_db
    from pegasus_v2f.db_schema import list_tables as _list_tables
    from rich.console import Console
    from rich.table import Table as RichTable

    db_arg = ctx.obj.get("db")
    config = None
    root = None
    if not db_arg:
        root = find_project_root(ctx.obj.get("project"))
        if root:
            config = read_config(root)

    with open_db(db=db_arg, config=config, read_only=True, project_root=root) as conn:
        tbl_list = _list_tables(conn)

    if not tbl_list:
        click.echo("No tables found. Use 'v2f source add' first.")
        return

    console = Console()
    tbl = RichTable(title="Tables")
    tbl.add_column("Table", style="bold")
    tbl.add_column("Rows", justify="right")

    for t in tbl_list:
        tbl.add_row(t["table"], f"{t['rows']:,}")

    console.print(tbl)


# =====================================================================
# Config group
# =====================================================================

@cli.group()
@click.pass_context
def config(ctx):
    """Manage configuration."""
    pass


@config.command("show")
@handle_errors
@click.pass_context
def config_show(ctx):
    """Show resolved config (all layers merged)."""
    from pegasus_v2f.project import find_project_root
    from pegasus_v2f.config import read_config, config_to_yaml

    root = find_project_root(ctx.obj.get("project"))
    if not root:
        raise click.ClickException("Not in a v2f project (no v2f.yaml found)")

    cfg = read_config(root)
    click.echo(config_to_yaml(cfg))


@config.command("edit")
@handle_errors
@click.option("--local", is_flag=True, help="Edit .v2f/local.yaml instead.")
@click.pass_context
def config_edit(ctx, local):
    """Open config in editor."""
    from pegasus_v2f.project import find_project_root

    root = find_project_root(ctx.obj.get("project"))
    if not root:
        raise click.ClickException("Not in a v2f project (no v2f.yaml found)")

    if local:
        target = root / ".v2f" / "local.yaml"
        if not target.exists():
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text("# Local overrides (not tracked in git)\n")
    else:
        target = root / "v2f.yaml"

    click.edit(filename=str(target))


@config.command("diff")
@handle_errors
@click.pass_context
def config_diff(ctx):
    """Show what local.yaml overrides."""
    import yaml
    from pegasus_v2f.project import find_project_root
    from rich.console import Console

    root = find_project_root(ctx.obj.get("project"))
    if not root:
        raise click.ClickException("Not in a v2f project (no v2f.yaml found)")

    local_path = root / ".v2f" / "local.yaml"
    if not local_path.exists():
        click.echo("No local overrides (.v2f/local.yaml not found).")
        return

    try:
        local = yaml.safe_load(local_path.read_text()) or {}
    except yaml.YAMLError as e:
        raise click.ClickException(f"Could not parse local config: {e}")

    console = Console()

    def _show_diff(d: dict, prefix: str = "") -> None:
        for key, val in d.items():
            path = f"{prefix}.{key}" if prefix else key
            if isinstance(val, dict):
                _show_diff(val, path)
            else:
                console.print(f"  [bold]{path}[/bold] = {val}")

    console.print("[bold]Local overrides (.v2f/local.yaml):[/bold]")
    _show_diff(local)


@config.command("validate")
@handle_errors
@click.pass_context
def config_validate(ctx):
    """Check config structure."""
    from pegasus_v2f.project import find_project_root
    from pegasus_v2f.config import read_config, validate_config
    from rich.console import Console

    root = find_project_root(ctx.obj.get("project"))
    if not root:
        raise click.ClickException("Not in a v2f project (no v2f.yaml found)")

    cfg = read_config(root)
    errors = validate_config(cfg)
    console = Console()
    if errors:
        for e in errors:
            console.print(f"  [red]✗[/red] {e}")
        raise click.ClickException(f"{len(errors)} validation error(s)")
    else:
        console.print("[green]✓[/green] Config is valid")


# =====================================================================
# Export group
# =====================================================================

@cli.group()
@click.pass_context
def export(ctx):
    """Export data."""
    pass


@export.command("csv")
@handle_errors
@click.argument("table")
@click.option("--output", "-o", default=None, help="Output file (default: stdout).")
@click.pass_context
def export_csv(ctx, table, output):
    """Export a table as CSV."""
    import csv
    import sys
    from pegasus_v2f.project import find_project_root
    from pegasus_v2f.config import read_config
    from pegasus_v2f.db import open_db, is_postgres

    db_arg = ctx.obj.get("db")
    config = None
    root = None
    if not db_arg:
        root = find_project_root(ctx.obj.get("project"))
        if root:
            config = read_config(root)

    with open_db(db=db_arg, config=config, read_only=True, project_root=root) as conn:
        try:
            if is_postgres(conn):
                cur = conn.cursor()
                cur.execute(f'SELECT * FROM "{table}"')
                columns = [desc[0] for desc in cur.description]
                rows = cur.fetchall()
                cur.close()
            else:
                result = conn.execute(f'SELECT * FROM "{table}"')
                columns = [desc[0] for desc in result.description]
                rows = result.fetchall()
        except Exception as e:
            raise click.ClickException(f"Could not read table '{table}': {e}")

    try:
        dest = open(output, "w", newline="") if output else sys.stdout
    except OSError as e:
        raise click.ClickException(f"Could not open output file: {e}")

    try:
        writer = csv.writer(dest)
        writer.writerow(columns)
        writer.writerows(rows)
    finally:
        if output:
            dest.close()

    if output:
        click.echo(f"Exported {len(rows)} rows to {output}")


@export.command("pegasus")
@handle_errors
@click.argument("study_name")
@click.option("--output", "-o", default=None, help="Output directory (default: current dir).")
@click.pass_context
def export_pegasus(ctx, study_name, output):
    """Export PEGASUS deliverables (evidence matrix, metadata, PEG list).

    STUDY_NAME is the study name (e.g. 'shrine_2023') or a direct study_id.
    """
    from pathlib import Path
    from pegasus_v2f.project import find_project_root
    from pegasus_v2f.config import read_config
    from pegasus_v2f.db import open_db
    from pegasus_v2f.pegasus_export import export_all

    db_arg = ctx.obj.get("db")
    config = None
    root = None
    if not db_arg:
        root = find_project_root(ctx.obj.get("project"))
        if root:
            config = read_config(root)

    output_dir = Path(output) if output else Path.cwd()

    try:
        with open_db(db=db_arg, config=config, read_only=True, project_root=root) as conn:
            paths = export_all(conn, study_name, output_dir)
    except (ValueError, FileNotFoundError) as e:
        raise click.ClickException(str(e))

    for name, path in paths.items():
        click.echo(f"  {name}: {path}")


    # NOTE: _pick_column, _integrate_locus_source, _integrate_evidence_source,
    # and _integrate_multi_evidence helpers were removed along with the
    # `source integrate` command. Evidence mapping now happens via `source add`
    # with evidence: blocks in v2f.yaml config.
