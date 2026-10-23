import click

from iomaps.commands.admin import admin
from iomaps.commands.extract import extract
from iomaps.commands.pyqt_ui import ui
from iomaps.commands.search import search
from iomaps.commands.sources import categories, sources


@click.group()
def main_cli():
    """Main command for Indianopenmaps CLI"""
    pass


@click.group(name="cli")
def cli():
    """CLI subcommands for Indianopenmaps"""
    pass


cli.add_command(extract)
cli.add_command(sources)
cli.add_command(categories)
cli.add_command(search)
main_cli.add_command(cli)
main_cli.add_command(admin)
main_cli.add_command(ui)

if __name__ == "__main__":
    main_cli()
