from importlib import import_module

import click

ADMIN_COMMANDS = {
    "make-release": ("iomaps.commands.admin_release", "make_release"),
    "prepare": ("iomaps.commands.admin_prepare", "prepare"),
}


class AdminGroup(click.Group):
    def list_commands(self, ctx):
        return sorted(ADMIN_COMMANDS)

    def get_command(self, ctx, command_name):
        target = ADMIN_COMMANDS.get(command_name)
        if target is None:
            return None

        module_name, attribute_name = target
        try:
            module = import_module(module_name)
        except ModuleNotFoundError as exc:
            if exc.name in {"duckdb", "github", "questionary"}:
                raise click.ClickException("Admin dependencies are not installed; install iomaps[admin]") from exc
            raise
        return getattr(module, attribute_name)


@click.group(cls=AdminGroup, hidden=True)
def admin():
    """Administrative commands."""
    pass
