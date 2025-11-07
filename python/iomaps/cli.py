import click

import subprocess
from pathlib import Path

from iomaps.pyqt_ui import main as run_pyqt_app

from iomaps.commands.filter_7z import filter_7z
from iomaps.commands.infer_schema import infer_schema

@click.group(invoke_without_command=True)
def main_cli():
    """Main command for Indianopenmaps CLI"""
    if click.get_current_context().invoked_subcommand is None:
        run_pyqt_app()

@click.group(name="cli")
def cli():
    """CLI subcommands for Indianopenmaps"""
    pass

cli.add_command(filter_7z)
cli.add_command(infer_schema)
main_cli.add_command(cli)

@click.command("ui")
def ui_command():
    """Launch the PyQt UI for Indianopenmaps"""
    run_pyqt_app()

main_cli.add_command(ui_command)

if __name__ == "__main__":
    main_cli()
