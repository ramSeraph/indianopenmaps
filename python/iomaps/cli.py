import click

from iomaps.commands.extract import extract
from iomaps.commands.filter_7z import filter_7z
from iomaps.commands.infer_schema import infer_schema
from iomaps.commands.pyqt_ui import ui
from iomaps.commands.sources import categories, sources


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
cli.add_command(extract)
cli.add_command(sources)
cli.add_command(categories)
main_cli.add_command(cli)
main_cli.add_command(ui)

if __name__ == "__main__":
    main_cli()
