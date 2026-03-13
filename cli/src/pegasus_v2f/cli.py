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
        {"name": "Commands", "commands": ["list", "inspect", "configure", "load", "show", "update", "remove"]},
    ],
    "v2f study": [
        {"name": "Commands", "commands": ["list", "inspect", "configure", "load", "show", "preview", "set", "remove"]},
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
    # via `v2f source configure` + `v2f source load`, loci via `v2f study configure` + `v2f study load`, and scoring via `v2f rescore`.
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
                    console.print("\n[bold]Config changed — building...[/bold]")
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
def build(ctx):
    """Build the database from v2f.yaml config.

    Creates the DB from scratch: schema, studies (with loci from stored
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
    report = Report(operation="build")

    conn = get_connection(db=db_arg, config=config, project_root=root)
    try:
        if has_tables(conn):
            if not click.confirm("This will delete and build the database from scratch. Continue?", default=False):
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
                transformations=study_cfg.get("transformations"),
                cache_dir=root / ".v2f",
                config_path=None,  # Don't re-write yaml during build
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
    """Re-run integration scoring (without full build)."""
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
        click.echo("No sources configured. Use 'v2f source configure' to add one.")
        return

    console = Console()
    table = RichTable(title="Data Sources")
    table.add_column("Name", style="bold")
    table.add_column("Type")
    table.add_column("Display Name")

    for s in sources:
        table.add_row(s["name"], s.get("source_type", ""), s.get("display_name", s["name"]))

    console.print(table)


def _render_proposed_config(config: dict, entity_type: str, console=None) -> None:
    """Render a proposed config block as YAML to stderr."""
    import sys
    import yaml
    from rich.console import Console
    from rich.panel import Panel
    from rich.syntax import Syntax

    if console is None:
        console = Console(stderr=True, file=sys.stderr)

    yaml_str = yaml.dump(config, default_flow_style=False, sort_keys=False)
    syntax = Syntax(yaml_str, "yaml", theme="monokai", word_wrap=True)
    console.print(Panel(syntax, title=f"Proposed {entity_type} config", border_style="dim"))


@source.command("configure")
@handle_errors
@click.argument("data_file")
@click.option("--name", default=None, help="Source name (defaults to filename stem).")
@click.option("--type", "source_type", type=click.Choice(["googlesheets", "file", "excel", "url"]))
@click.option("--sheet", default=None, help="Sheet/tab name.")
@click.option("--skip", "skip_rows", default=None, type=int, help="Rows to skip before header.")
@click.option("--gene-column", default=None, help="Column containing gene symbols.")
@click.option("--category", default=None, help="PEGASUS evidence category.")
@click.option("--traits", default=None, help="Comma-separated trait tags.")
@click.option("--centric", type=click.Choice(["gene", "variant"]), default=None)
@click.option("--source-tag", default=None, help="Source tag identifier.")
@click.option("--display-name", default=None, help="Human-readable display name.")
@click.option("--transform-json", default=None, help="JSON list of transformations.")
@click.option("--evidence-json", default=None, help="JSON list of evidence blocks (overrides auto-proposal).")
@click.option("--force", is_flag=True, help="Overwrite existing config entry.")
@click.option("--ai", "use_ai", is_flag=True, help="AI-assisted suggestions.")
@click.option("--json", "json_output", is_flag=True, help="Output structured JSON.")
@click.pass_context
def source_configure(ctx, data_file, name, source_type, sheet, skip_rows, gene_column,
                     category, traits, centric, source_tag, display_name, transform_json,
                     evidence_json, force, use_ai, json_output):
    """Propose config for a data source, validate, and write to v2f.yaml.

    DATA_FILE is a local file path or URL. Never touches the database.
    """
    from pathlib import Path
    from pegasus_v2f.project import find_project_root
    from pegasus_v2f.config import read_config, append_source_to_yaml, remove_source_from_yaml
    from pegasus_v2f.loaders import load_source
    from pegasus_v2f.propose import propose_source_config
    from pegasus_v2f.validate import validate_source, render_validation

    root = find_project_root(ctx.obj.get("project"))
    if not root:
        raise click.ClickException("Not in a v2f project (no v2f.yaml found)")

    config_path = root / "v2f.yaml"
    data_dir = root / "data" / "raw"
    if not data_dir.exists():
        data_dir = root

    # Derive source name from filename if not provided
    if not name:
        name = Path(data_file).stem.replace(" ", "_").lower()

    # Build source_def for loading
    if not source_type:
        if data_file.startswith("http"):
            if "docs.google.com/spreadsheets" in data_file:
                source_type = "googlesheets"
            else:
                source_type = "url"
        elif data_file.endswith((".xlsx", ".xls")):
            source_type = "excel"
        else:
            source_type = "file"

    source_def: dict = {"name": name, "source_type": source_type}
    if source_type in ("googlesheets", "url"):
        source_def["url"] = data_file
    else:
        source_def["path"] = data_file
    if sheet:
        source_def["sheet"] = sheet
    if skip_rows is not None:
        source_def["skip_rows"] = skip_rows
    if gene_column:
        source_def["gene_column"] = gene_column

    # Load data
    try:
        df = load_source(source_def, data_dir=data_dir)
    except Exception as e:
        raise click.ClickException(f"Could not load data: {e}")

    # Parse transform-json
    if transform_json:
        import json as _json
        try:
            transforms = _json.loads(transform_json)
        except _json.JSONDecodeError as e:
            raise click.ClickException(f"Invalid --transform-json: {e}")
        source_def["transformations"] = transforms

    # AI suggestion
    ai_suggestion = None
    if use_ai:
        from pegasus_v2f.ai_assist import get_provider
        from pegasus_v2f.inspect import inspect_dataframe
        provider = get_provider("auto")
        if provider:
            inspection_for_ai = inspect_dataframe(df, source_name=name)
            ai_suggestion = provider.suggest(inspection_for_ai, heuristic_fixes=inspection_for_ai.suggested_fixes)

    # Parse evidence-json
    explicit_evidence = None
    if evidence_json:
        import json as _json
        try:
            explicit_evidence = _json.loads(evidence_json)
        except _json.JSONDecodeError as e:
            raise click.ClickException(f"Invalid --evidence-json: {e}")

    # Propose config
    proposed, inspection = propose_source_config(
        df, name, source_def,
        ai_suggestion=ai_suggestion,
        gene_column=gene_column,
        category=category,
        traits=traits,
        centric=centric,
        source_tag=source_tag,
    )

    # Override evidence blocks if explicitly provided
    if explicit_evidence is not None:
        proposed["evidence"] = explicit_evidence

    # Set display name if provided
    if display_name:
        proposed["display_name"] = display_name

    # Validate
    validation = validate_source(proposed, data_dir=data_dir, df=df)

    is_json = json_output or ctx.obj.get("json_output")

    if not is_json:
        # Render results
        from pegasus_v2f.inspect import render_inspection
        render_inspection(inspection)
        _render_proposed_config(proposed, "source")
        render_validation(validation)

    if not validation.is_valid:
        if is_json:
            import json as _json
            print(_json.dumps({
                "error": "validation_failed",
                "proposed_config": proposed,
                "validation": validation.to_dict(),
            }, indent=2))
        raise click.ClickException("Config not written. Fix errors and re-run.")

    # Check for existing entry
    config = read_config(root)
    existing = [s for s in config.get("data_sources", []) if s["name"] == name]
    if existing and not force:
        raise click.ClickException(f"Source '{name}' already in v2f.yaml. Use --force to replace.")

    # Write to v2f.yaml
    if existing and force:
        remove_source_from_yaml(config_path, name)
    append_source_to_yaml(config_path, proposed)

    if is_json:
        import json as _json
        output = {
            "proposed_config": proposed,
            "validation": validation.to_dict(),
            "inspection": inspection.to_dict(),
            "written": True,
        }
        print(_json.dumps(output, indent=2))
    else:
        click.echo(f"Wrote config for source '{name}' to v2f.yaml")


@source.command("load")
@handle_errors
@click.argument("name")
@click.option("--force", is_flag=True, help="Replace existing DB entry.")
@click.option("--no-score", is_flag=True, help="Skip auto-scoring.")
@click.option("-y", "--yes", is_flag=True, help="Skip confirmation.")
@click.option("--json", "json_output", is_flag=True, help="Output structured JSON.")
@click.pass_context
def source_load(ctx, name, force, no_score, yes, json_output):
    """Load a configured source from v2f.yaml into the database.

    Reads the source config by NAME, validates against data, then loads.
    """
    from pegasus_v2f.project import find_project_root
    from pegasus_v2f.config import read_config, get_data_sources
    from pegasus_v2f.db import open_db
    from pegasus_v2f import sources as src_mod
    from pegasus_v2f.validate import validate_source, render_validation

    root = find_project_root(ctx.obj.get("project"))
    if not root:
        raise click.ClickException("Not in a v2f project (no v2f.yaml found)")

    config = read_config(root)
    sources = get_data_sources(config)
    source_config = next((s for s in sources if s["name"] == name), None)
    if not source_config:
        raise click.ClickException(f"Source '{name}' not found in v2f.yaml")

    data_dir = root / "data" / "raw"
    if not data_dir.exists():
        data_dir = root

    # Validate before loading
    validation = validate_source(source_config, data_dir=data_dir)

    if json_output or ctx.obj.get("json_output"):
        import json as _json
        if not validation.is_valid:
            print(_json.dumps({"error": "validation_failed", "validation": validation.to_dict()}, indent=2))
            raise click.ClickException("Validation failed. Fix errors and re-run.")
        # Continue to load and output result as JSON below
    else:
        render_validation(validation)

    if not validation.is_valid:
        raise click.ClickException("Config not loaded. Fix errors and re-run.")

    if not yes and not json_output:
        if not click.confirm(f"Load source '{name}' into database?", default=True):
            raise SystemExit(0)

    db_arg = ctx.obj.get("db")
    from pegasus_v2f.report import Report, render_report
    report = Report(operation="source_load")

    try:
        with open_db(db=db_arg, config=config, project_root=root) as conn:
            from pegasus_v2f.pegasus_schema import create_pegasus_schema
            create_pegasus_schema(conn)
            if force:
                try:
                    src_mod.remove_source(conn, name, config=config)
                except ValueError:
                    pass
            rows = src_mod.add_source(conn, source_config, data_dir=data_dir, config=config,
                                      no_score=no_score, report=report)
    except (ValueError, FileNotFoundError) as e:
        raise click.ClickException(str(e))

    if json_output or ctx.obj.get("json_output"):
        import json as _json
        print(_json.dumps({"name": name, "rows": rows, "validation": validation.to_dict()}, indent=2))
    else:
        click.echo(f"Loaded source '{name}': {rows} rows")
        if report.has_warnings:
            render_report(report)


@source.command("inspect")
@handle_errors
@click.argument("data_file")
@click.option("--name", default=None, help="Source name (defaults to filename stem).")
@click.option("--type", "source_type", type=click.Choice(["googlesheets", "file", "excel", "url"]))
@click.option("--sheet", default=None, help="Sheet/tab name.")
@click.option("--skip", "skip_rows", default=None, type=int, help="Rows to skip before header.")
@click.option("--ai", "use_ai", is_flag=True, help="Enable AI-assisted category suggestion.")
@click.option("--json", "json_output", is_flag=True, help="Output structured JSON.")
@click.pass_context
def source_inspect(ctx, data_file, name, source_type, sheet, skip_rows, use_ai, json_output):
    """Inspect a data source for PEGASUS compatibility (read-only).

    Shows data profile AND a proposed config block. No side effects.
    DATA_FILE is a local file path or URL.
    """
    from pathlib import Path
    from pegasus_v2f.project import find_project_root
    from pegasus_v2f.loaders import load_source
    from pegasus_v2f.inspect import inspect_dataframe, render_inspection
    from pegasus_v2f.propose import propose_source_config

    if not name:
        name = Path(data_file).stem.replace(" ", "_").lower()

    # Auto-detect source type
    if not source_type:
        if data_file.startswith("http"):
            if "docs.google.com/spreadsheets" in data_file:
                source_type = "googlesheets"
            else:
                source_type = "url"
        elif data_file.endswith((".xlsx", ".xls")):
            source_type = "excel"
        else:
            source_type = "file"

    source_def: dict = {"name": name, "source_type": source_type}
    if source_type in ("googlesheets", "url"):
        source_def["url"] = data_file
    else:
        source_def["path"] = data_file
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
        if data_file.startswith("http"):
            from rich.console import Console
            console = Console(stderr=True)
            with console.status("Downloading source..."):
                df = load_source(source_def, data_dir=data_dir)
        else:
            df = load_source(source_def, data_dir=data_dir)
    except Exception as e:
        raise click.ClickException(f"Could not load source: {e}")

    # AI suggestion
    ai_suggestion = None
    if use_ai:
        from pegasus_v2f.ai_assist import get_provider
        provider = get_provider("auto")
        if provider:
            inspection_for_ai = inspect_dataframe(df, source_name=name)
            ai_suggestion = provider.suggest(inspection_for_ai, heuristic_fixes=inspection_for_ai.suggested_fixes)
            if not ai_suggestion:
                click.echo("AI suggestion failed — showing heuristic only.", err=True)
        else:
            click.echo("AI provider not available (claude CLI not found).", err=True)

    # Propose config (includes inspection)
    proposed, inspection = propose_source_config(
        df, name, source_def, ai_suggestion=ai_suggestion,
    )

    if json_output or ctx.obj.get("json_output"):
        import json as _json
        output = inspection.to_dict()
        output["proposed_config"] = proposed
        if ai_suggestion:
            output["ai_suggestion"] = ai_suggestion.to_dict()
        print(_json.dumps(output, indent=2))
        return

    render_inspection(inspection)
    _render_proposed_config(proposed, "source")

    if use_ai and ai_suggestion:
        _show_ai_suggestion_display(ai_suggestion)


def _show_ai_suggestion_display(suggestion):
    """Display an AI suggestion (non-interactive)."""
    from pegasus_v2f.pegasus_schema import EVIDENCE_CATEGORIES
    from rich.console import Console
    import sys

    console = Console(stderr=True, file=sys.stderr)

    console.print(f"\n  [bold]AI Analysis[/bold]")
    if suggestion.category:
        label = EVIDENCE_CATEGORIES.get(suggestion.category, "")
        console.print(f"    Category: [bold]{suggestion.category}[/bold] ({label})")
    if suggestion.category_reasoning:
        console.print(f"    \"{suggestion.category_reasoning}\"")
    if suggestion.column_mappings:
        mappings_str = ", ".join(f"{k}->{v}" for k, v in suggestion.column_mappings.items())
        console.print(f"    Mappings: {mappings_str}")
    console.print(f"    Centric: {suggestion.centric}")
    if suggestion.quality_notes:
        console.print(f"\n    Notes:")
        for note in suggestion.quality_notes:
            console.print(f"    - {note}")
    if suggestion.transformations:
        console.print(f"\n    Transformations:")
        for t in suggestion.transformations:
            col = t.get("column", t.get("columns", "N/A"))
            if isinstance(col, dict):
                col = ", ".join(f"{k}->{v}" for k, v in col.items())
            elif isinstance(col, list):
                col = ", ".join(col)
            console.print(f"    - {t['type']} on {col}")
    console.print(f"    Confidence: {suggestion.confidence:.0%}\n")


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
        click.echo("No studies configured. Use 'v2f study configure' to create one.")
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


@study.command("inspect")
@handle_errors
@click.argument("loci_file")
@click.option("--sheet", default=None, help="Sheet/tab name (for Excel or Google Sheets).")
@click.option("--skip", "skip_rows", default=None, type=int, help="Rows to skip before header.")
@click.option("--window-kb", type=int, default=500, help="Locus window half-size in kb (default: 500).")
@click.option("--merge-kb", type=int, default=250, help="Merge distance in kb (default: 250).")
@click.option("--chr-column", default=None, help="Specify chromosome column name.")
@click.option("--pos-column", default=None, help="Specify position column name.")
@click.option("--gene-column", default=None, help="Specify gene column name.")
@click.option("--pvalue-column", default=None, help="Specify p-value column name.")
@click.option("--rsid-column", default=None, help="Specify rsID column name.")
@click.option("--sentinel-column", default=None, help="Specify variant ID column name.")
@click.option("--ai", "use_ai", is_flag=True, help="Enable AI-assisted analysis.")
@click.option("--json", "json_output", is_flag=True, help="Output structured JSON.")
@click.pass_context
def study_inspect(ctx, loci_file, sheet, skip_rows, window_kb, merge_kb,
                  chr_column, pos_column,
                  gene_column, pvalue_column, rsid_column, sentinel_column,
                  use_ai, json_output):
    """Inspect sentinel variant data before adding a study.

    Profiles columns, validates positions, detects traits, and previews
    how sentinels will cluster into loci. LOCI_FILE can be a local path
    or a Google Sheets URL.
    """
    from pathlib import Path

    import pandas as pd

    from pegasus_v2f.project import find_project_root
    from pegasus_v2f.study_inspect import inspect_sentinels, render_study_inspection

    root = find_project_root(ctx.obj.get("project"))
    cache_dir = root / ".v2f" if root else None

    # Load sentinel data — same logic as study_add
    loci_df = None
    if loci_file.startswith("http"):
        from pegasus_v2f.loaders import load_googlesheets

        from rich.console import Console

        console = Console(stderr=True)
        source_spec = {"url": loci_file, "source_type": "googlesheets"}
        if sheet:
            source_spec["sheet"] = sheet
        if skip_rows:
            source_spec["skip_rows"] = skip_rows
        try:
            with console.status("Downloading spreadsheet..."):
                loci_df = load_googlesheets(source_spec)
        except Exception as e:
            raise click.ClickException(f"Could not load Google Sheet: {e}")
    else:
        loci_path = Path(loci_file)
        if not loci_path.exists():
            raise click.ClickException(f"File not found: {loci_path}")

        if loci_path.suffix.lower() in (".xlsx", ".xls"):
            kwargs = {"engine": "calamine"}
            if sheet:
                kwargs["sheet_name"] = sheet
            if skip_rows:
                kwargs["skiprows"] = skip_rows
            loci_df = pd.read_excel(loci_path, **kwargs)
        elif loci_path.suffix.lower() in (".tsv", ".gz"):
            loci_df = pd.read_csv(loci_path, sep="\t")
        else:
            loci_df = pd.read_csv(loci_path)

    if loci_df is None or loci_df.empty:
        raise click.ClickException("No data loaded from sentinel file")

    label = sheet or loci_file.rstrip("/").split("/")[-1] or loci_file

    # AI suggestion (used for both column detection and proposed config)
    ai_suggestion = None
    if use_ai and not (chr_column and pos_column):
        from pegasus_v2f.ai_assist import get_provider
        from pegasus_v2f.study_inspect import _detect_sentinel_columns

        # Quick heuristic check first — only call AI if heuristics miss chr/pos
        quick_det = _detect_sentinel_columns(loci_df)
        if not (chr_column or quick_det.chromosome) or not (pos_column or quick_det.position):
            provider = get_provider("auto")
            if provider:
                from pegasus_v2f.inspect import _profile_columns

                click.echo("Auto-detection missed key columns, asking AI...", err=True)
                col_summaries = [c.to_dict() for c in _profile_columns(loci_df)]
                ai_cols = provider.suggest_columns(col_summaries, context="sentinel")
                if ai_cols and ai_cols.mappings:
                    if not chr_column and "chromosome" in ai_cols.mappings:
                        chr_column = ai_cols.mappings["chromosome"]
                        click.echo(f"  AI detected chromosome -> {chr_column}", err=True)
                    if not pos_column and "position" in ai_cols.mappings:
                        pos_column = ai_cols.mappings["position"]
                        click.echo(f"  AI detected position -> {pos_column}", err=True)
                    if not gene_column and "gene" in ai_cols.mappings:
                        gene_column = ai_cols.mappings["gene"]
                        click.echo(f"  AI detected gene -> {gene_column}", err=True)
                    if not pvalue_column and "pvalue" in ai_cols.mappings:
                        pvalue_column = ai_cols.mappings["pvalue"]
                        click.echo(f"  AI detected pvalue -> {pvalue_column}", err=True)
                    if not rsid_column and "rsid" in ai_cols.mappings:
                        rsid_column = ai_cols.mappings["rsid"]
                        click.echo(f"  AI detected rsid -> {rsid_column}", err=True)

    result = inspect_sentinels(
        loci_df,
        source_label=label,
        window_kb=window_kb,
        merge_distance_kb=merge_kb,
        cache_dir=cache_dir,
        chr_col=chr_column,
        pos_col=pos_column,
        gene_col=gene_column,
        pvalue_col=pvalue_column,
        rsid_col=rsid_column,
        sentinel_col=sentinel_column,
    )

    # Warn if key columns still missing
    det = result.column_detection
    if not det.chromosome or not det.position:
        missing = []
        if not det.chromosome:
            missing.append("chromosome")
        if not det.position:
            missing.append("position")
        click.echo(
            f"Warning: Could not detect {', '.join(missing)} column(s). "
            f"Clustering preview skipped.\n"
            f"  Use --chr-column/--pos-column to specify, or --ai for AI detection.",
            err=True,
        )

    # Generate proposed config for display
    from pegasus_v2f.propose import propose_study_config
    label = sheet or loci_file.rstrip("/").split("/")[-1] or loci_file
    study_name = Path(loci_file).stem.replace(" ", "_").lower() if not loci_file.startswith("http") else label

    # Build a proposed config (with placeholder traits since inspect is read-only)
    det = result.column_detection
    proposed, _ = propose_study_config(
        loci_df, study_name, ["<TRAIT>"],
        loci_source=loci_file,
        loci_sheet=sheet,
        loci_skip=skip_rows,
        window_kb=window_kb,
        merge_distance_kb=merge_kb,
        cache_dir=cache_dir,
        gene_column=gene_column or det.gene,
        sentinel_column=sentinel_column or det.sentinel_id,
        pvalue_column=pvalue_column or det.pvalue,
        rsid_column=rsid_column or det.rsid,
        ai_suggestion=ai_suggestion,
    )

    if json_output or ctx.obj.get("json_output"):
        import json as _json
        output = result.to_dict()
        output["proposed_config"] = proposed
        if ai_suggestion:
            output["ai_suggestion"] = ai_suggestion.to_dict()
        print(_json.dumps(output, indent=2))
        return

    render_study_inspection(result)
    _render_proposed_config(proposed, "study")

    if use_ai and ai_suggestion:
        _show_ai_suggestion_display(ai_suggestion)


@study.command("configure")
@handle_errors
@click.argument("loci_file")
@click.option("--name", default=None, help="Study name (defaults to filename stem).")
@click.option("--traits", "traits_str", required=True, help="Comma-separated trait names.")
@click.option("--sheet", "loci_sheet", default=None, help="Sheet/tab name.")
@click.option("--skip", "loci_skip", default=None, type=int, help="Rows to skip before header.")
@click.option("--gene-column", default=None, help="Gene column name.")
@click.option("--sentinel-column", default=None, help="Variant ID column name.")
@click.option("--pvalue-column", default=None, help="P-value column name.")
@click.option("--rsid-column", default=None, help="rsID column name.")
@click.option("--gwas-source", default=None, help="GWAS source (PMID or GCST).")
@click.option("--ancestry", default=None, help="Population ancestry.")
@click.option("--sex", default=None, help="Sex restriction.")
@click.option("--sample-size", type=int, default=None, help="GWAS sample size.")
@click.option("--doi", default=None, help="Publication DOI.")
@click.option("--year", type=int, default=None, help="Publication year.")
@click.option("--window-kb", type=int, default=500, help="Locus window half-size in kb.")
@click.option("--merge-kb", type=int, default=250, help="Merge distance in kb.")
@click.option("--transform-json", default=None, help="JSON list of transformations.")
@click.option("--force", is_flag=True, help="Overwrite existing config entry.")
@click.option("--ai", "use_ai", is_flag=True, help="AI-assisted suggestions.")
@click.option("--json", "json_output", is_flag=True, help="Output structured JSON.")
@click.pass_context
def study_configure(ctx, loci_file, name, traits_str, loci_sheet, loci_skip,
                    gene_column, sentinel_column, pvalue_column, rsid_column,
                    gwas_source, ancestry, sex, sample_size, doi, year,
                    window_kb, merge_kb, transform_json, force, use_ai, json_output):
    """Propose config for a study, validate, and write to v2f.yaml.

    LOCI_FILE is a local file path or URL. --traits is required.
    Never touches the database.
    """
    from pathlib import Path
    import pandas as pd
    from pegasus_v2f.project import find_project_root
    from pegasus_v2f.config import read_config, add_study_to_yaml, remove_study_from_yaml, get_study_list
    from pegasus_v2f.propose import propose_study_config
    from pegasus_v2f.validate import validate_study, render_validation
    from pegasus_v2f.study_inspect import render_study_inspection

    root = find_project_root(ctx.obj.get("project"))
    if not root:
        raise click.ClickException("Not in a v2f project (no v2f.yaml found)")

    config_path = root / "v2f.yaml"
    cache_dir = root / ".v2f"

    # Derive study name
    if not name:
        if loci_file.startswith("http"):
            name = "study"
        else:
            name = Path(loci_file).stem.replace(" ", "_").lower()

    traits = [t.strip() for t in traits_str.split(",") if t.strip()]
    if not traits:
        raise click.ClickException("At least one trait is required")

    # Load sentinel data
    loci_df = _load_loci_file(loci_file, loci_sheet, loci_skip)

    # AI suggestion
    ai_suggestion = None
    if use_ai:
        from pegasus_v2f.ai_assist import get_provider
        from pegasus_v2f.study_inspect import inspect_sentinels
        provider = get_provider("auto")
        if provider:
            inspection_for_ai = inspect_sentinels(loci_df, source_label=name, cache_dir=cache_dir)
            ai_suggestion = provider.suggest(inspection_for_ai, heuristic_fixes=inspection_for_ai.suggested_fixes)

    # Propose config
    proposed, inspection = propose_study_config(
        loci_df, name, traits, loci_source=loci_file,
        loci_sheet=loci_sheet, loci_skip=loci_skip,
        window_kb=window_kb, merge_distance_kb=merge_kb,
        ai_suggestion=ai_suggestion, cache_dir=cache_dir,
        gene_column=gene_column, sentinel_column=sentinel_column,
        pvalue_column=pvalue_column, rsid_column=rsid_column,
        gwas_source=gwas_source, ancestry=ancestry, sex=sex,
        sample_size=sample_size, doi=doi, year=year,
    )

    # Apply transform-json override
    if transform_json:
        import json as _json
        try:
            transforms = _json.loads(transform_json)
        except _json.JSONDecodeError as e:
            raise click.ClickException(f"Invalid --transform-json: {e}")
        proposed["transformations"] = transforms

    # Validate
    config = read_config(root)
    locus_def = config.get("pegasus", {}).get("locus_definition", {})
    validation = validate_study(proposed, locus_def=locus_def, df=loci_df)

    is_json = json_output or ctx.obj.get("json_output")

    if not is_json:
        # Render results
        render_study_inspection(inspection)
        _render_proposed_config(proposed, "study")
        render_validation(validation)

    if not validation.is_valid:
        if is_json:
            import json as _json
            print(_json.dumps({
                "error": "validation_failed",
                "proposed_config": proposed,
                "validation": validation.to_dict(),
            }, indent=2))
        raise click.ClickException("Config not written. Fix errors and re-run.")

    # Check for existing entry
    existing = [s for s in get_study_list(config) if s.get("id_prefix") == name]
    if existing and not force:
        raise click.ClickException(f"Study '{name}' already in v2f.yaml. Use --force to replace.")

    # Write to v2f.yaml
    if existing and force:
        remove_study_from_yaml(config_path, name)

    locus_config = None
    if not config.get("pegasus", {}).get("locus_definition"):
        locus_config = {"window_kb": window_kb, "merge_distance_kb": merge_kb}

    try:
        add_study_to_yaml(config_path, proposed, locus_config)
    except ValueError as e:
        raise click.ClickException(str(e))

    if is_json:
        import json as _json
        output = {
            "proposed_config": proposed,
            "validation": validation.to_dict(),
            "inspection": inspection.to_dict(),
            "written": True,
        }
        print(_json.dumps(output, indent=2))
    else:
        click.echo(f"Wrote config for study '{name}' to v2f.yaml")


@study.command("load")
@handle_errors
@click.argument("name")
@click.option("--force", is_flag=True, help="Replace existing DB entry.")
@click.option("-y", "--yes", is_flag=True, help="Skip confirmation.")
@click.option("--json", "json_output", is_flag=True, help="Output structured JSON.")
@click.pass_context
def study_load(ctx, name, force, yes, json_output):
    """Load a configured study from v2f.yaml into the database.

    Reads the study config by NAME, validates against data, then loads.
    """
    from pegasus_v2f.project import find_project_root
    from pegasus_v2f.config import read_config, get_study_by_id, remove_study_from_yaml
    from pegasus_v2f.db import get_connection
    from pegasus_v2f.validate import validate_study, render_validation

    root = find_project_root(ctx.obj.get("project"))
    if not root:
        raise click.ClickException("Not in a v2f project (no v2f.yaml found)")

    config = read_config(root)
    study_config = get_study_by_id(config, name)
    if not study_config:
        raise click.ClickException(f"Study '{name}' not found in v2f.yaml")

    locus_def = config.get("pegasus", {}).get("locus_definition", {})

    # Validate before loading
    validation = validate_study(study_config, locus_def=locus_def)

    if json_output or ctx.obj.get("json_output"):
        import json as _json
        if not validation.is_valid:
            print(_json.dumps({"error": "validation_failed", "validation": validation.to_dict()}, indent=2))
            raise click.ClickException("Validation failed. Fix errors and re-run.")
    else:
        render_validation(validation)

    if not validation.is_valid:
        raise click.ClickException("Config not loaded. Fix errors and re-run.")

    if not yes and not json_output:
        if not click.confirm(f"Load study '{name}' into database?", default=True):
            raise SystemExit(0)

    # Load into DB
    db_arg = ctx.obj.get("db")
    config_path = root / "v2f.yaml"

    from pegasus_v2f.pegasus_schema import create_pegasus_schema
    from pegasus_v2f.study_management import add_study as _add_study, remove_study as _remove_study
    from pegasus_v2f.report import Report, render_report

    report = Report(operation="study_load")
    conn = get_connection(db=db_arg, config=config, project_root=root)

    try:
        create_pegasus_schema(conn)

        if force:
            try:
                _remove_study(conn, name)
            except ValueError:
                pass

        result = _add_study(
            conn,
            study_name=name,
            traits=study_config.get("traits", []),
            loci_source=study_config.get("loci_source"),
            loci_sheet=study_config.get("loci_sheet"),
            loci_skip=study_config.get("loci_skip"),
            gwas_source=study_config.get("gwas_source"),
            ancestry=study_config.get("ancestry"),
            sex=study_config.get("sex"),
            sample_size=study_config.get("sample_size"),
            doi=study_config.get("doi"),
            year=study_config.get("year"),
            genome_build=study_config.get("genome_build", config.get("database", {}).get("genome_build", "hg38")),
            gene_column=study_config.get("gene_column"),
            sentinel_column=study_config.get("sentinel_column"),
            pvalue_column=study_config.get("pvalue_column"),
            rsid_column=study_config.get("rsid_column"),
            window_kb=locus_def.get("window_kb", 500),
            merge_distance_kb=locus_def.get("merge_distance_kb", 250),
            transformations=study_config.get("transformations"),
            cache_dir=root / ".v2f",
            config_path=None,  # Don't re-write yaml (already configured)
            report=report,
        )

        if json_output or ctx.obj.get("json_output"):
            import json as _json
            print(_json.dumps({
                "name": name,
                "n_loci": result["n_loci"],
                "n_sentinels": result["n_sentinels"],
                "validation": validation.to_dict(),
            }, indent=2))
        else:
            click.echo(
                f"Loaded study '{name}': "
                f"{result['n_loci']} loci from {result['n_sentinels']} sentinels"
            )
            if report.has_warnings:
                render_report(report)
    finally:
        conn.close()


def _load_loci_file(loci_file, loci_sheet=None, loci_skip=None):
    """Load sentinel data from a file path or URL. Shared by study commands."""
    from pathlib import Path
    import pandas as pd

    if loci_file.startswith("http"):
        from pegasus_v2f.loaders import load_googlesheets
        source_spec = {"url": loci_file, "source_type": "googlesheets"}
        if loci_sheet:
            source_spec["sheet"] = loci_sheet
        if loci_skip:
            source_spec["skip_rows"] = loci_skip
        return load_googlesheets(source_spec)

    loci_path = Path(loci_file)
    if not loci_path.exists():
        raise click.ClickException(f"File not found: {loci_path}")

    if loci_path.suffix.lower() in (".xlsx", ".xls"):
        kwargs = {"engine": "calamine"}
        if loci_sheet:
            kwargs["sheet_name"] = loci_sheet
        if loci_skip:
            kwargs["skiprows"] = loci_skip
        return pd.read_excel(loci_path, **kwargs)
    elif loci_path.suffix.lower() in (".tsv", ".gz"):
        return pd.read_csv(loci_path, sep="\t")
    else:
        return pd.read_csv(loci_path)


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
        click.echo("No tables found. Use 'v2f source configure' + 'v2f source load' first.")
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
    # `source integrate` command. Evidence mapping now happens via `source configure`
    # with evidence: blocks in v2f.yaml config.
