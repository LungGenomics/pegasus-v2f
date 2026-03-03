"""Main CLI entry point for v2f."""

import rich_click as click

from pegasus_v2f import __version__

click.rich_click.USE_RICH_MARKUP = True
click.rich_click.GROUP_ARGUMENTS_OPTIONS = True
click.rich_click.COMMAND_GROUPS = {
    "v2f": [
        {"name": "Build & Sources", "commands": ["build", "add-source", "update-source", "remove-source", "sources", "materialize", "integrate"]},
        {"name": "Server", "commands": ["serve"]},
        {"name": "Project", "commands": ["init", "status", "sync"]},
        {"name": "Query & Inspect", "commands": ["query", "tables", "config", "export"]},
    ]
}


# --- Global options ---

@click.group()
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


# --- Build commands ---

@cli.command()
@click.option("--from-db", default=None, help="Rebuild using config stored in an existing database.")
@click.option("--overwrite", is_flag=True, help="Drop and rebuild if DB already has tables.")
@click.pass_context
def build(ctx, from_db, overwrite):
    """Build database from config."""
    import logging
    from pegasus_v2f.project import find_project_root
    from pegasus_v2f.config import read_config
    from pegasus_v2f.db import get_connection, open_db
    from pegasus_v2f.db_meta import read_meta
    from pegasus_v2f.pipeline import build_db
    import yaml

    logging.basicConfig(level=logging.WARNING if ctx.obj.get("quiet") else logging.INFO)

    # Get config: from existing DB or from project files
    if from_db:
        with open_db(db=from_db, read_only=True) as src:
            config_yaml = read_meta(src, "config")
            if not config_yaml:
                raise click.ClickException("No config found in source database")
            config = yaml.safe_load(config_yaml)
        project_root = None
    else:
        root = find_project_root(ctx.obj.get("project"))
        if not root:
            raise click.ClickException("Not in a v2f project (no v2f.yaml found)")
        config = read_config(root)
        project_root = root

    db_arg = ctx.obj.get("db")
    with open_db(db=db_arg, config=config, project_root=project_root) as conn:
        result = build_db(conn, config, project_root=project_root, overwrite=overwrite)

    click.echo(f"Built {result['sources_loaded']}/{result['sources_total']} sources, {result['genes_found']} genes")


@cli.command("add-source")
@click.argument("name")
@click.option("--type", "source_type", type=click.Choice(["googlesheets", "file", "excel", "url"]))
@click.option("--url", default=None, help="Source URL (Google Sheets or remote file).")
@click.option("--path", "file_path", default=None, help="Local file path.")
@click.option("--gene-column", default="gene", help="Column containing gene symbols.")
@click.option("--display-name", default=None, help="Human-readable display name.")
@click.option("--no-score", is_flag=True, help="Skip auto-scoring (for batch operations).")
@click.pass_context
def add_source(ctx, name, source_type, url, file_path, gene_column, display_name, no_score):
    """Add a data source."""
    from pegasus_v2f.project import find_project_root
    from pegasus_v2f.config import read_config
    from pegasus_v2f.db import open_db
    from pegasus_v2f import sources as src_mod

    source = {"name": name, "source_type": source_type or "file"}
    if url:
        source["url"] = url
    if file_path:
        source["path"] = file_path
    if gene_column != "gene":
        source["gene_column"] = gene_column
    if display_name:
        source["display_name"] = display_name

    db_arg = ctx.obj.get("db")
    config = None
    data_dir = None
    root = find_project_root(ctx.obj.get("project"))
    if root:
        config = read_config(root)
        data_dir = root / "data" / "raw"
        if not data_dir.exists():
            data_dir = root

    with open_db(db=db_arg, config=config, project_root=root) as conn:
        rows = src_mod.add_source(conn, source, data_dir=data_dir, config=config, no_score=no_score)

    click.echo(f"Added source '{name}': {rows} rows")


@cli.command("update-source")
@click.argument("name")
@click.pass_context
def update_source(ctx, name):
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

    with open_db(db=db_arg, config=config, project_root=root) as conn:
        rows = src_mod.update_source(conn, name, data_dir=data_dir)

    click.echo(f"Updated source '{name}': {rows} rows")


@cli.command("remove-source")
@click.argument("name")
@click.pass_context
def remove_source(ctx, name):
    """Drop table and remove source from config."""
    from pegasus_v2f.project import find_project_root
    from pegasus_v2f.config import read_config
    from pegasus_v2f.db import open_db
    from pegasus_v2f import sources as src_mod

    db_arg = ctx.obj.get("db")
    config = None
    root = find_project_root(ctx.obj.get("project"))
    if root:
        config = read_config(root)

    with open_db(db=db_arg, config=config, project_root=root) as conn:
        src_mod.remove_source(conn, name)

    click.echo(f"Removed source '{name}'")


@cli.command()
@click.pass_context
def sources(ctx):
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
        source_list = src_mod.list_sources(conn)

    if not source_list:
        click.echo("No sources configured.")
        return

    console = Console()
    table = RichTable(title="Data Sources")
    table.add_column("Name", style="bold")
    table.add_column("Type")
    table.add_column("Display Name")

    for s in source_list:
        table.add_row(s["name"], s.get("source_type", ""), s.get("display_name", s["name"]))

    console.print(table)


# --- Server ---

@cli.command()
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


# --- Project management ---

@cli.command()
@click.argument("url", required=False)
@click.pass_context
def init(ctx, url):
    """Initialize a new project (optionally from a git URL)."""
    from pathlib import Path
    from pegasus_v2f.project import init_project

    dest = Path.cwd() if not url else Path.cwd() / Path(url).stem
    root = init_project(dest, url=url)
    click.echo(f"Initialized v2f project at {root}")


@cli.command()
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
                    console.print("\n[yellow]Config changed[/yellow] — run [bold]v2f build --overwrite[/bold] to rebuild")
            else:
                console.print("[green]Up to date[/green]")

    except RuntimeError as e:
        raise click.ClickException(str(e))


# --- Query / inspect ---

@cli.command()
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
@click.pass_context
def tables(ctx):
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
        click.echo("No tables found.")
        return

    console = Console()
    table = RichTable(title="Tables")
    table.add_column("Table", style="bold")
    table.add_column("Rows", justify="right")

    for t in tbl_list:
        table.add_row(t["table"], f"{t['rows']:,}")

    console.print(table)


# --- Config ---

@cli.group()
@click.pass_context
def config(ctx):
    """Manage configuration."""
    pass


@config.command("show")
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

    local = yaml.safe_load(local_path.read_text()) or {}
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
@click.pass_context
def config_validate(ctx):
    """Check config structure."""
    from pegasus_v2f.project import find_project_root
    from pegasus_v2f.config import read_config, validate_config

    root = find_project_root(ctx.obj.get("project"))
    if not root:
        raise click.ClickException("Not in a v2f project (no v2f.yaml found)")

    cfg = read_config(root)
    errors = validate_config(cfg)
    if errors:
        for e in errors:
            click.echo(f"  [red]✗[/red] {e}")
        raise click.ClickException(f"{len(errors)} validation error(s)")
    else:
        click.echo("[green]✓[/green] Config is valid")


# --- Export ---

@cli.group()
@click.pass_context
def export(ctx):
    """Export data."""
    pass


@export.command("csv")
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
            raise click.ClickException(f"Failed to read table '{table}': {e}")

    dest = open(output, "w", newline="") if output else sys.stdout
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
@click.argument("study_id")
@click.option("--output", "-o", default=None, help="Output directory (default: current dir).")
@click.pass_context
def export_pegasus(ctx, study_id, output):
    """Export PEGASUS deliverables (evidence matrix, metadata, PEG list)."""
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

    with open_db(db=db_arg, config=config, read_only=True, project_root=root) as conn:
        paths = export_all(conn, study_id, output_dir)

    for name, path in paths.items():
        click.echo(f"  {name}: {path}")


@cli.command()
@click.pass_context
def materialize(ctx):
    """Re-run integration scoring (without full rebuild)."""
    import logging
    from pegasus_v2f.project import find_project_root
    from pegasus_v2f.config import read_config
    from pegasus_v2f.db import open_db
    from pegasus_v2f.scoring import compute_locus_gene_scores

    logging.basicConfig(level=logging.WARNING if ctx.obj.get("quiet") else logging.INFO)

    db_arg = ctx.obj.get("db")
    config = None
    root = find_project_root(ctx.obj.get("project"))
    if root:
        config = read_config(root)

    if not config or not config.get("pegasus"):
        raise click.ClickException("No pegasus config found — materialize requires a PEGASUS build")

    with open_db(db=db_arg, config=config, project_root=root) as conn:
        n = compute_locus_gene_scores(conn, config)

    click.echo(f"Scored {n} locus-gene pairs")


@cli.command()
@click.argument("source_name")
@click.option("--category", default=None, help="Evidence category (e.g. COLOC, QTL, GWAS).")
@click.option("--centric", default=None, type=click.Choice(["gene", "variant"]), help="Evidence centric type.")
@click.option("--source-tag", default=None, help="Source tag for provenance.")
@click.pass_context
def integrate(ctx, source_name, category, centric, source_tag):
    """Map a raw table to PEGASUS evidence categories.

    Walks through column mapping interactively (or use --category/--centric/--source-tag
    flags for non-interactive use). Updates v2f.yaml in place, loads evidence,
    drops the raw table, and re-runs scoring.
    """
    import logging
    from pegasus_v2f.project import find_project_root
    from pegasus_v2f.config import read_config
    from pegasus_v2f.db import open_db
    from pegasus_v2f.integrate import (
        detect_columns,
        suggest_mappings,
        validate_mapping,
        apply_integration,
    )
    from pegasus_v2f.pegasus_schema import EVIDENCE_CATEGORIES

    logging.basicConfig(level=logging.WARNING if ctx.obj.get("quiet") else logging.INFO)

    db_arg = ctx.obj.get("db")
    root = find_project_root(ctx.obj.get("project"))
    if not root:
        raise click.ClickException("Not in a v2f project (no v2f.yaml found)")

    config = read_config(root)
    config_path = root / "v2f.yaml"

    with open_db(db=db_arg, config=config, project_root=root) as conn:
        # Detect columns
        try:
            columns = detect_columns(conn, source_name)
        except Exception as e:
            raise click.ClickException(f"Could not read table '{source_name}': {e}")

        suggestions = suggest_mappings(columns, source_name)

        # Interactive prompts (if flags not provided)
        if not category:
            click.echo(f"\nColumns in '{source_name}':")
            for c in columns:
                samples = ", ".join(c["sample_values"][:3]) if c["sample_values"] else "—"
                click.echo(f"  {c['name']} ({c['type']}): {samples}")

            if suggestions["category"]:
                category = click.prompt(
                    "\nEvidence category",
                    default=suggestions["category"],
                )
            else:
                click.echo(f"\nValid categories: {', '.join(sorted(EVIDENCE_CATEGORIES))}")
                category = click.prompt("Evidence category")

        if not centric:
            centric = click.prompt(
                "Centric type",
                default=suggestions["centric"],
                type=click.Choice(["gene", "variant"]),
            )

        if not source_tag:
            source_tag = click.prompt("Source tag", default=source_name)

        # Build field mappings interactively
        fields = {}
        suggested = suggestions["fields"]

        gene_default = suggested.get("gene", "")
        fields["gene"] = click.prompt("Gene column", default=gene_default) if not gene_default else gene_default

        if centric == "variant":
            chr_default = suggested.get("chromosome", "")
            pos_default = suggested.get("position", "")
            if chr_default:
                fields["chromosome"] = chr_default
            else:
                fields["chromosome"] = click.prompt("Chromosome column")
            if pos_default:
                fields["position"] = pos_default
            else:
                fields["position"] = click.prompt("Position column")

        # Optional fields
        for opt_field in ["pvalue", "score", "effect_size", "tissue", "rsid"]:
            if opt_field in suggested:
                fields[opt_field] = suggested[opt_field]

        mapping = {
            "category": category,
            "centric": centric,
            "source_tag": source_tag,
            "fields": fields,
        }

        # Validate
        errors = validate_mapping(mapping)
        if errors:
            for e in errors:
                click.echo(f"  Error: {e}")
            raise click.ClickException("Invalid mapping")

        # Confirm
        click.echo(f"\nMapping: {category} ({centric}) → source_tag={source_tag}")
        click.echo(f"Fields: {fields}")
        if not click.confirm("Apply?", default=True):
            click.echo("Cancelled.")
            return

        # Apply
        result = apply_integration(conn, source_name, mapping, config, config_path=config_path)

    click.echo(f"\nIntegrated '{source_name}' as {category} ({centric})")
    if result["scores_computed"]:
        click.echo(f"Re-scored {result['scores_computed']} locus-gene pairs")
