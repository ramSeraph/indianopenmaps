import click

import subprocess
from pathlib import Path

#from iomaps.ui import run_streamlit_app

from iomaps.commands.filter_7z import filter_7z
from iomaps.commands.infer_schema import infer_schema

@click.group()
def main_cli():
    """Main command for Indianopenmaps CLI"""
    pass

@click.group(name="cli")
def cli():
    """CLI subcommands for Indianopenmaps"""
    pass

cli.add_command(filter_7z)
cli.add_command(infer_schema)
main_cli.add_command(cli)

@click.command("ui")
def ui_command():
    """Launch the Streamlit UI for Indianopenmaps"""
    script_path = Path(__file__).parent / "ui.py"
    subprocess.run(["streamlit", "run", str(script_path)])

main_cli.add_command(ui_command)

if __name__ == "__main__":
    main_cli()
