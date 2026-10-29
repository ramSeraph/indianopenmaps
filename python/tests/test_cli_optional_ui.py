import builtins
import json
import os
import subprocess
import sys
import textwrap
from types import SimpleNamespace

from click.testing import CliRunner

from iomaps.cli import main_cli


def test_headless_commands_never_import_qt(tmp_path):
    routes = tmp_path / "routes.json"
    routes.write_text(json.dumps({"/states/soi/": {"name": "SOI States", "category": "states"}}))
    # Use a fresh process so another test's Qt import cannot hide eager loading.
    script = textwrap.dedent(
        """
        import importlib.abc
        import json
        import sys
        from click.testing import CliRunner

        attempted = []
        class NoQt(importlib.abc.MetaPathFinder):
            def find_spec(self, fullname, path=None, target=None):
                if fullname == 'PyQt5' or fullname.startswith('PyQt5.'):
                    attempted.append(fullname)
                    raise ModuleNotFoundError('Qt deliberately unavailable', name=fullname)

        sys.meta_path.insert(0, NoQt())
        from iomaps.cli import main_cli
        runner = CliRunner()
        for args in [[], ['--help'], ['cli', '--help'], ['cli', 'extract', '--help'],
                     ['cli', 'search', '--help'], ['admin', '--help'], ['ui', '--help']]:
            result = runner.invoke(main_cli, args)
            assert result.exit_code == (2 if not args else 0), result.output
        result = runner.invoke(main_cli, ['cli', 'sources', '--routes-file', sys.argv[1], '--json'])
        assert result.exit_code == 0, result.output
        assert [s['route'] for s in json.loads(result.stdout)] == ['/states/soi/']
        assert attempted == []
        assert 'iomaps.commands.pyqt_ui' not in sys.modules

        result = runner.invoke(main_cli, ['ui'])
        assert result.exit_code == 1, result.output
        assert 'iomaps[ui]' in result.output
        assert 'uvx --from' in result.output
        assert attempted == ['PyQt5']
        """
    )
    result = subprocess.run(
        [sys.executable, "-c", script, str(routes)],
        env={**os.environ, "PYTHONDONTWRITEBYTECODE": "1"},
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert result.returncode == 0, result.stdout + result.stderr


def test_ui_launches_only_when_requested(monkeypatch):
    calls = []
    monkeypatch.setitem(sys.modules, "iomaps.commands.pyqt_ui", SimpleNamespace(main=lambda: calls.append("opened")))
    runner = CliRunner()
    assert runner.invoke(main_cli, ["--help"]).exit_code == 0
    assert runner.invoke(main_cli, ["ui", "--help"]).exit_code == 0
    assert calls == []
    result = runner.invoke(main_cli, ["ui"])
    assert result.exit_code == 0, result.output
    assert calls == ["opened"]


def test_ui_does_not_hide_unrelated_import_errors(monkeypatch):
    original_import = builtins.__import__
    failure = ModuleNotFoundError("Unrelated UI dependency", name="unrelated_module")

    def import_with_error(name, *args, **kwargs):
        if name == "iomaps.commands.pyqt_ui":
            raise failure
        return original_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", import_with_error)
    result = CliRunner().invoke(main_cli, ["ui"])
    assert result.exception is failure
    assert "Install it with" not in result.output
