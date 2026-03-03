"""Main CLI entry point for v2f."""

import rich_click as click

from pegasus_v2f import __version__

click.rich_click.USE_RICH_MARKUP = True
click.rich_click.GROUP_ARGUMENTS_OPTIONS = True
click.rich_click.COMMAND_GROUPS = {
    "v2f": [
        {"name": "Build & Sources", "commands": ["build", "add-source", "update-source", "remove-source", "sources", "materialize", "integrate"]},
        {"name": "Server", "commands": ["serve"]},
        {"name": "Project", "commands": ["init", "status", "sync", "study"]},
        {"name": "Query & Inspect", "commands": ["query", "tables", "studies", "config", "export"]},
    ],
    "v2f study": [
        {"name": "Commands", "commands": ["list", "add"]},
    ],
}


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
@click.option("--sheet", default=None, help="Sheet/tab name (for Google Sheets or Excel with multiple sheets).")
@click.option("--skip", "skip_rows", default=None, type=int, help="Rows to skip before header (skips preview).")
@click.option("--gene-column", default="gene", help="Column containing gene symbols.")
@click.option("--display-name", default=None, help="Human-readable display name.")
@click.option("--no-score", is_flag=True, help="Skip auto-scoring (for batch operations).")
@click.option("--force", is_flag=True, help="Replace source if it already exists.")
@click.pass_context
def add_source(ctx, name, source_type, url, file_path, sheet, skip_rows, gene_column, display_name, no_score, force):
    """Add a data source.

    Shows a preview of the first rows so you can confirm which row is the
    header. Use --skip to bypass the preview.
    """
    from pegasus_v2f.project import find_project_root
    from pegasus_v2f.config import read_config
    from pegasus_v2f.db import open_db
    from pegasus_v2f import sources as src_mod

    source = {"name": name, "source_type": source_type or "file"}
    if url:
        source["url"] = url
    if file_path:
        source["path"] = file_path
    if sheet:
        source["sheet"] = sheet
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

    # Check for duplicates before downloading/previewing
    if not force:
        try:
            with open_db(db=db_arg, config=config, project_root=root) as conn:
                existing = src_mod.list_sources(conn)
                if any(s["name"] == name for s in existing):
                    raise click.ClickException(f"Source '{name}' already exists. Use --force to replace it.")
        except click.ClickException:
            raise
        except Exception:
            pass  # DB might not exist yet, that's fine

    # Preview: always show so the user can verify the header
    import pandas as pd
    from pegasus_v2f.loaders import preview_source
    from rich.console import Console
    from rich.table import Table as RichTable

    console = Console()
    is_gsheet = (source.get("source_type") == "googlesheets")

    try:
        if is_gsheet:
            with console.status("Downloading spreadsheet..."):
                preview = preview_source(source, data_dir=data_dir, n_rows=10)
        else:
            preview = preview_source(source, data_dir=data_dir, n_rows=10)
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
    else:
        click.echo(f"\nHeader row: {skip_rows}")
        if not click.confirm("Proceed?", default=True):
            raise SystemExit(0)

    if skip_rows:
        source["skip_rows"] = skip_rows

    try:
        with open_db(db=db_arg, config=config, project_root=root) as conn:
            if force:
                try:
                    src_mod.remove_source(conn, name, config=config)
                except ValueError:
                    pass  # Source didn't exist, that's fine
            rows = src_mod.add_source(conn, source, data_dir=data_dir, config=config, no_score=no_score)
    except (ValueError, FileNotFoundError) as e:
        raise click.ClickException(str(e))

    # Write source to v2f.yaml so file config stays in sync with DB
    if root:
        from pegasus_v2f.config import append_source_to_yaml, remove_source_from_yaml
        config_path = root / "v2f.yaml"
        if config_path.exists():
            if force:
                remove_source_from_yaml(config_path, name)
            append_source_to_yaml(config_path, source)

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

    try:
        with open_db(db=db_arg, config=config, project_root=root) as conn:
            rows = src_mod.update_source(conn, name, data_dir=data_dir)
    except ValueError as e:
        raise click.ClickException(str(e))

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
        click.echo("No sources configured. Use 'v2f add-source' to add one.")
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
    try:
        root = init_project(dest, url=url)
    except FileExistsError as e:
        raise click.ClickException(str(e))
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
def studies(ctx):
    """List configured studies."""
    ctx.invoke(study_list)


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
        click.echo("No tables found. Use 'v2f add-source' or 'v2f build' first.")
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

    try:
        with open_db(db=db_arg, config=config, read_only=True, project_root=root) as conn:
            paths = export_all(conn, study_id, output_dir)
    except (ValueError, FileNotFoundError) as e:
        raise click.ClickException(str(e))

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
@click.option("--role", default=None, type=click.Choice(["locus_definition", "gwas_sumstats"]),
              help="Structural role (for locus sources).")
@click.option("--category", default=None, help="Evidence category (e.g. COLOC, QTL, GWAS).")
@click.option("--centric", default=None, type=click.Choice(["gene", "variant"]), help="Evidence centric type.")
@click.option("--source-tag", default=None, help="Source tag for provenance.")
@click.pass_context
def integrate(ctx, source_name, role, category, centric, source_tag):
    """Map a raw table to PEGASUS evidence.

    Walks through column mapping interactively (or use flags for non-interactive
    use). Updates v2f.yaml in place, loads evidence, drops the raw table, and
    re-runs scoring.
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
        suggested = suggestions["fields"]
        col_names = [c["name"] for c in columns]

        # Step 1: Determine source type (role vs evidence)
        import questionary

        if not role and not category:
            click.echo(f"\nColumns in '{source_name}':")
            for c in columns:
                samples = ", ".join(c["sample_values"][:3]) if c["sample_values"] else "—"
                click.echo(f"  {c['name']} ({c['type']}): {samples}")

            default_kind = suggestions.get("role") or "evidence"
            source_kind = questionary.select(
                "\nSource type:",
                choices=[
                    questionary.Choice("Gene/variant evidence", value="evidence"),
                    questionary.Choice("Locus definition (curated GWAS loci)", value="locus_definition"),
                    questionary.Choice("GWAS summary statistics", value="gwas_sumstats"),
                ],
                default=default_kind,
            ).ask()
            if source_kind is None:
                raise SystemExit(0)
            if source_kind in ("locus_definition", "gwas_sumstats"):
                role = source_kind

        if role:
            # --- Locus source path ---
            mapping = _integrate_locus_source(
                source_name, role, source_tag, suggested, col_names, config, config_path, root,
            )
        else:
            # --- Evidence source path ---
            mapping = _integrate_evidence_source(
                source_name, category, centric, source_tag, suggested, col_names, suggestions,
                config=config,
            )

        # Validate
        errors = validate_mapping(mapping)
        if errors:
            for e in errors:
                click.echo(f"  Error: {e}")
            raise click.ClickException("Invalid mapping")

        # Confirm
        if mapping.get("role"):
            click.echo(f"\nMapping: {mapping['role']} → source_tag={mapping['source_tag']}")
        else:
            click.echo(f"\nMapping: {mapping['category']} ({mapping['centric']}) → source_tag={mapping['source_tag']}")
        click.echo(f"Fields: {mapping['fields']}")
        if not click.confirm("Apply?", default=True):
            click.echo("Cancelled.")
            return

        # Re-read config in case we just wrote pegasus study config
        config = read_config(root)

        # Apply
        result = apply_integration(conn, source_name, mapping, config, config_path=config_path)

    if mapping.get("role"):
        click.echo(f"\nIntegrated '{source_name}' as {mapping['role']}")
    else:
        click.echo(f"\nIntegrated '{source_name}' as {mapping['category']} ({mapping['centric']})")
    if result.get("scores_computed"):
        click.echo(f"Re-scored {result['scores_computed']} locus-gene pairs")


# ---------------------------------------------------------------------------
# v2f study — multi-study management
# ---------------------------------------------------------------------------

class StudyGroup(click.RichGroup):
    """Custom group that handles fixed commands (list, add) and dynamic id_prefix dispatch.

    Any unrecognized subcommand is treated as a study id_prefix, giving access
    to per-study commands (show, set, remove, trait).
    """

    def get_command(self, ctx, cmd_name):
        # Try fixed subcommands first (list, add)
        rv = super().get_command(ctx, cmd_name)
        if rv is not None:
            return rv
        # Treat anything else as a study id_prefix
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
            ("<id> show", "Show study details"),
            ("<id> set <key> <value>", "Update field (gwas_source, ancestry, genome_build)"),
            ("<id> remove", "Remove study"),
            ("<id> trait list", "List traits"),
            ("<id> trait add <trait>", "Add a trait"),
            ("<id> trait remove <trait>", "Remove a trait"),
        ]:
            table.add_row(cmd, desc)

        panel = RichClickRichPanel(table, title="Per-Study Commands", border_style="dim")
        formatter.write(panel)


def _make_study_id_group(id_prefix: str) -> click.Group:
    """Create a dynamic Click group for a specific study id_prefix."""

    @click.group(name=id_prefix, invoke_without_command=True)
    @click.pass_context
    def study_id_group(ctx):
        """Commands for a specific study."""
        ctx.ensure_object(dict)
        ctx.obj["study_id_prefix"] = id_prefix
        if ctx.invoked_subcommand is None:
            # Default to show
            ctx.invoke(study_show)

    @study_id_group.command("show")
    @click.pass_context
    def study_show(ctx):
        """Show details for this study."""
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
        for key in ("genome_build", "gwas_source", "ancestry"):
            if study.get(key):
                console.print(f"[bold]{key}:[/bold] {study[key]}")

    @study_id_group.command("set")
    @click.argument("key")
    @click.argument("value")
    @click.pass_context
    def study_set(ctx, key, value):
        """Update a study field (gwas_source, ancestry, genome_build)."""
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

    @study_id_group.command("remove")
    @click.pass_context
    def study_remove(ctx):
        """Remove this study from config."""
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

    # --- Nested trait group ---
    @study_id_group.group("trait")
    @click.pass_context
    def trait_group(ctx):
        """Manage traits for this study."""
        ctx.ensure_object(dict)
        ctx.obj["study_id_prefix"] = id_prefix

    @trait_group.command("list")
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

    return study_id_group


@cli.group(cls=StudyGroup)
@click.pass_context
def study(ctx):
    """Manage PEGASUS studies."""
    pass


@study.command("list")
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

    for s in studies:
        table.add_row(
            s.get("id_prefix", ""),
            ", ".join(s.get("traits", [])),
            s.get("genome_build", ""),
            s.get("gwas_source", ""),
            s.get("ancestry", ""),
        )

    console.print(table)


@study.command("add")
@click.pass_context
def study_add(ctx):
    """Add a new study interactively."""
    import questionary
    from pegasus_v2f.project import find_project_root
    from pegasus_v2f.config import read_config, add_study_to_yaml, get_study_list

    root = find_project_root(ctx.obj.get("project"))
    if not root:
        raise click.ClickException("Not in a v2f project (no v2f.yaml found)")

    config_path = root / "v2f.yaml"

    id_prefix = questionary.text("Study ID prefix (e.g. shrine_2023):").ask()
    if id_prefix is None:
        raise SystemExit(0)

    traits_str = questionary.text("Traits (comma-separated, e.g. FEV1,FVC):").ask()
    if traits_str is None:
        raise SystemExit(0)
    traits = [t.strip() for t in traits_str.split(",") if t.strip()]
    if not traits:
        raise click.ClickException("At least one trait is required")

    genome_build = questionary.text("Genome build:", default="GRCh38").ask()
    if genome_build is None:
        raise SystemExit(0)

    study_config = {
        "id_prefix": id_prefix,
        "traits": traits,
        "genome_build": genome_build,
    }

    gwas_source = questionary.text("GWAS source (PMID or GCST, optional):").ask()
    if gwas_source:
        study_config["gwas_source"] = gwas_source
    ancestry = questionary.text("Ancestry (optional):").ask()
    if ancestry:
        study_config["ancestry"] = ancestry

    # Offer locus_definition setup if none exists
    config = read_config(root)
    locus_config = None
    if not config.get("pegasus", {}).get("locus_definition"):
        if questionary.confirm("Set up locus definition parameters?", default=True).ask():
            window_kb = questionary.text("Locus window (kb):", default="500").ask()
            if window_kb is None:
                raise SystemExit(0)
            merge_kb = questionary.text("Merge distance (kb):", default="250").ask()
            if merge_kb is None:
                raise SystemExit(0)
            locus_config = {
                "window_kb": int(window_kb),
                "merge_distance_kb": int(merge_kb),
            }

    try:
        add_study_to_yaml(config_path, study_config, locus_config)
    except ValueError as e:
        raise click.ClickException(str(e))

    click.echo(f"Added study '{id_prefix}' with traits: {', '.join(traits)}")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _pick_column(label: str, col_names: list[str], default: str | None = None) -> str:
    """Prompt user to select a column, or accept the auto-detected default."""
    import questionary
    if default and default in col_names:
        return default
    result = questionary.select(f"{label}:", choices=col_names).ask()
    if result is None:
        raise SystemExit(0)
    return result


def _integrate_locus_source(source_name, role, source_tag, suggested, col_names, config, config_path, root):
    """Interactive prompts for locus_definition / gwas_sumstats integration."""
    import questionary
    from pegasus_v2f.config import get_study_list, add_study_to_yaml

    if not source_tag:
        source_tag = source_name

    fields = {}

    # Gene column (not required for gwas_sumstats)
    if role == "locus_definition":
        fields["gene"] = _pick_column("Gene column", col_names, suggested.get("gene"))

    # Trait column (for locus_definition without a fixed trait)
    if role == "locus_definition":
        has_trait_col = suggested.get("trait") in col_names if suggested.get("trait") else False
        if has_trait_col or questionary.confirm("Does this source have a trait column?", default=True).ask():
            fields["trait"] = _pick_column("Trait column", col_names, suggested.get("trait"))

    # Chromosome + position
    fields["chromosome"] = _pick_column("Chromosome column", col_names, suggested.get("chromosome"))
    fields["position"] = _pick_column("Position column", col_names, suggested.get("position"))

    # Optional fields
    for opt_field in ["pvalue", "rsid", "effect_size", "sentinel"]:
        if opt_field in suggested:
            fields[opt_field] = suggested[opt_field]

    # Ensure at least one study exists
    studies = get_study_list(config)
    if not studies and config_path:
        click.echo("\nNo pegasus study config found. Let's set one up.")
        id_prefix = questionary.text("Study ID prefix (e.g. shrine_2023):", default=source_tag).ask()
        if id_prefix is None:
            raise SystemExit(0)
        traits_str = questionary.text("Traits (comma-separated, e.g. FEV1,FVC):").ask()
        if traits_str is None:
            raise SystemExit(0)
        traits = [t.strip() for t in traits_str.split(",") if t.strip()]
        genome_build = questionary.text("Genome build:", default="GRCh38").ask()
        if genome_build is None:
            raise SystemExit(0)

        study_config = {
            "id_prefix": id_prefix,
            "traits": traits,
            "genome_build": genome_build,
        }

        gwas_source = questionary.text("GWAS source (PMID or GCST, optional):").ask()
        if gwas_source:
            study_config["gwas_source"] = gwas_source
        ancestry = questionary.text("Ancestry (optional):").ask()
        if ancestry:
            study_config["ancestry"] = ancestry

        window_kb = questionary.text("Locus window (kb):", default="500").ask()
        if window_kb is None:
            raise SystemExit(0)
        merge_kb = questionary.text("Merge distance (kb):", default="250").ask()
        if merge_kb is None:
            raise SystemExit(0)

        locus_config = {
            "window_kb": int(window_kb),
            "merge_distance_kb": int(merge_kb),
        }

        add_study_to_yaml(config_path, study_config, locus_config)
        click.echo(f"Wrote pegasus study config to {config_path}")
        studies = [study_config]

    # Select study (auto-select if only one)
    mapping = {
        "role": role,
        "source_tag": source_tag,
        "fields": fields,
    }

    if len(studies) == 1:
        mapping["study"] = studies[0]["id_prefix"]
    elif len(studies) > 1:
        choices = [s["id_prefix"] for s in studies]
        selected = questionary.select("Which study does this source belong to?", choices=choices).ask()
        if selected is None:
            raise SystemExit(0)
        mapping["study"] = selected

    return mapping


def _integrate_evidence_source(source_name, category, centric, source_tag, suggested, col_names, suggestions, config=None):
    """Interactive prompts for category/centric evidence integration."""
    import questionary
    from pegasus_v2f.pegasus_schema import EVIDENCE_CATEGORIES
    from pegasus_v2f.config import get_study_list

    if not category:
        # Build category choices, putting suggestion first
        cat_choices = sorted(EVIDENCE_CATEGORIES.keys())
        category = questionary.select(
            "Evidence category:",
            choices=cat_choices,
            default=suggestions["category"] if suggestions["category"] else None,
        ).ask()
        if category is None:
            raise SystemExit(0)

    if not centric:
        centric = questionary.select(
            "Centric type:",
            choices=[
                questionary.Choice("gene (gene-level annotation)", value="gene"),
                questionary.Choice("variant (matched to loci by position)", value="variant"),
            ],
            default=suggestions["centric"],
        ).ask()
        if centric is None:
            raise SystemExit(0)

    if not source_tag:
        source_tag = source_name

    fields = {}

    fields["gene"] = _pick_column("Gene column", col_names, suggested.get("gene"))

    if centric == "variant":
        fields["chromosome"] = _pick_column("Chromosome column", col_names, suggested.get("chromosome"))
        fields["position"] = _pick_column("Position column", col_names, suggested.get("position"))

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

    # Ask about trait-specific evidence (gene-centric only)
    if config and centric == "gene":
        studies = get_study_list(config)
        if studies:
            # Collect all traits across all studies
            all_traits = []
            for s in studies:
                all_traits.extend(s.get("traits", []))
            all_traits = sorted(set(all_traits))

            if all_traits and questionary.confirm(
                "Is this evidence specific to a single trait?", default=False
            ).ask():
                trait = questionary.select("Which trait?", choices=all_traits).ask()
                if trait:
                    mapping["trait"] = trait

    return mapping
