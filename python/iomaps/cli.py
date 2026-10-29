import click

from iomaps.commands.admin import admin
from iomaps.commands.extract import extract
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


@click.command("ui")
def ui():
    """Launch the optional PyQt UI (requires iomaps[ui])."""
    try:
        from iomaps.commands.pyqt_ui import main
    except ModuleNotFoundError as exc:
        if exc.name == "PyQt5" or (exc.name and exc.name.startswith("PyQt5.")):
            raise click.ClickException(
                'The UI requires PyQt5. Install it with pip install "iomaps[ui]", '
                'or run uvx --from "iomaps[ui]" iomaps ui.'
            ) from exc
        raise
    main()


cli.add_command(extract)
cli.add_command(sources)
cli.add_command(categories)
cli.add_command(search)
main_cli.add_command(cli)
main_cli.add_command(admin)
main_cli.add_command(ui)

if __name__ == "__main__":
    main_cli()
