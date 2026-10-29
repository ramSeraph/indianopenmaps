import json

import pytest
from click.testing import CliRunner

from iomaps.cli import main_cli


def test_source_discovery_works_without_archive_commands(tmp_path):
    routes = tmp_path / "routes.json"
    routes.write_text(
        json.dumps(
            {
                "/states/soi/": {"name": "SOI States", "category": "states", "url": "https://example.org/data.pmtiles"},
                "/raster/": {"name": "Raster", "type": "raster"},
            }
        )
    )
    runner = CliRunner()
    result = runner.invoke(main_cli, ["cli", "sources", "--routes-file", str(routes), "--json"])
    assert result.exit_code == 0, result.output
    sources = json.loads(result.stdout)
    assert [s["route"] for s in sources] == ["/states/soi/"]
    for command in ("filter-7z", "infer-schema"):
        result = runner.invoke(main_cli, ["cli", command, "--help"])
        assert result.exit_code == 2
        assert "No such command" in result.output


def test_remote_extraction_ui_still_selects_sources_and_closes(monkeypatch):
    pytest.importorskip("PyQt5.QtWidgets", reason="Install iomaps[ui] to run UI tests")
    monkeypatch.setenv("QT_QPA_PLATFORM", "offscreen")
    from iomaps.commands.pyqt_ui import PyQtApp, QApplication

    app = QApplication.instance() or QApplication([])
    monkeypatch.setattr(PyQtApp, "_fetch_extract_sources", lambda self: None)
    window = PyQtApp()
    try:
        source = {"name": "SOI States", "route": "/states/soi/"}
        window._handle_sources_fetched([source])
        window._on_extract_source_selected(window.extract_source_list.item(0))
        assert window.extract_selected_source == source
        assert window.extract_output_filename_input.text() == "SOI_States.gpkg"
        window.extract_output_type_combo.setCurrentText("Parquet")
        assert window.extract_output_filename_input.text() == "SOI_States.parquet"
        assert window.content_stack.count() == 1
        window._log_status("Ready")
        assert "Ready" in window.extract_status_text_edit.toPlainText()
    finally:
        window.close()
        app.processEvents()
