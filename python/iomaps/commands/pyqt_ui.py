import shutil
import sys
import tempfile
from pathlib import Path

import click
from PyQt5.QtCore import QThread, pyqtSignal
from PyQt5.QtWidgets import (
    QApplication,
    QComboBox,
    QFileDialog,
    QGroupBox,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QListWidget,
    QListWidgetItem,
    QMainWindow,
    QProgressBar,
    QPushButton,
    QScrollArea,
    QStackedWidget,
    QTextEdit,
    QVBoxLayout,
    QWidget,
)


class SourceFetchWorker(QThread):
    """Worker to fetch sources list in background."""

    finished = pyqtSignal(object)
    error = pyqtSignal(str)

    def run(self):
        try:
            from iomaps.commands.sources import get_vector_sources

            sources = get_vector_sources()
            self.finished.emit(sources)
        except Exception as e:
            self.error.emit(str(e))


class ExtractWorker(QThread):
    """Worker for extract command."""

    status = pyqtSignal(str)
    finished = pyqtSignal(object)
    error = pyqtSignal(str)

    def __init__(self, source_config, bounds, output_file_path, output_driver):
        super().__init__()
        self.source_config = source_config
        self.bounds = bounds
        self.output_file_path = output_file_path
        self.output_driver = output_driver

    def run(self):
        try:
            from iomaps.commands.extract import (
                GDAL_FORMATS,
                build_source_query,
                fetch_partition_metadata,
                get_duckdb_connection,
                get_parquet_info_from_source,
                get_partitions_for_filter,
                write_csv_output,
                write_gdal_output,
                write_parquet_output,
            )
            from iomaps.core.spatial_filter import get_shape_from_bounds

            self.status.emit("Getting parquet info...")

            parquet_info = get_parquet_info_from_source(self.source_config)
            if parquet_info is None:
                self.error.emit("Could not determine parquet URL for source")
                return

            # Get filter shape from bounds if provided
            filter_shape = None
            if self.bounds:
                filter_shape = get_shape_from_bounds(self.bounds)

            # Collect parquet URLs
            parquet_urls = []

            if parquet_info["is_partitioned"]:
                self.status.emit("Fetching partition metadata...")
                partition_metadata = fetch_partition_metadata(parquet_info["meta_url"])
                if partition_metadata is None:
                    self.error.emit(f"Failed to fetch metadata for {parquet_info['base_url']}")
                    return

                partitions = get_partitions_for_filter(parquet_info["meta_url"], filter_shape, partition_metadata)
                if not partitions:
                    self.error.emit("No partitions intersect with the filter bounds.")
                    return
                self.status.emit(f"Found {len(partitions)} partitions to process")
                parquet_urls.extend(partitions)
            else:
                parquet_urls.append(parquet_info["base_url"])

            # Build query
            query = build_source_query(parquet_urls, filter_shape)

            # Create DuckDB connection
            self.status.emit("Connecting to DuckDB...")
            con = get_duckdb_connection()

            try:
                self.status.emit(f"Downloading and writing {self.output_driver}...")

                if self.output_driver in GDAL_FORMATS:
                    write_gdal_output(con, query, str(self.output_file_path), self.output_driver)
                elif self.output_driver == "CSV":
                    write_csv_output(con, query, str(self.output_file_path))
                elif self.output_driver == "Parquet":
                    write_parquet_output(con, query, str(self.output_file_path))
                else:
                    self.error.emit(f"Unsupported output driver: {self.output_driver}")
                    return

                self.finished.emit(True)
            finally:
                con.close()

        except Exception as e:
            self.error.emit(str(e))


class PyQtApp(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Indianopenmaps - Extract")
        self.setGeometry(100, 100, 800, 600)

        self._apply_dark_theme()

        self.central_widget = QWidget()
        self.setCentralWidget(self.central_widget)
        self.main_layout = QHBoxLayout(self.central_widget)

        self.content_stack = QStackedWidget()

        self.main_layout.addWidget(self.content_stack, 3)

        self.temp_dir_extract_obj = None

        # Internal state variables for Extract
        self.extract_sources = []  # Full list of sources
        self.extract_selected_source = None
        self.extract_output_file_path = None

        self._setup_extract_widget()

    def _apply_dark_theme(self):
        """Apply dark theme stylesheet."""
        self.setStyleSheet("""
            QMainWindow, QWidget {
                background-color: #1e1e1e;
                color: #d4d4d4;
            }
            QLabel {
                color: #d4d4d4;
                qproperty-alignment: AlignLeft;
            }
            QLabel a {
                color: #3794ff;
            }
            QPushButton {
                background-color: #0e639c;
                color: #ffffff;
                border: none;
                padding: 8px 16px;
                border-radius: 4px;
                font-weight: bold;
            }
            QPushButton:hover {
                background-color: #1177bb;
            }
            QPushButton:pressed {
                background-color: #0d5a8c;
            }
            QPushButton:disabled {
                background-color: #3c3c3c;
                color: #6e6e6e;
            }
            QLineEdit, QTextEdit {
                background-color: #2d2d2d;
                color: #d4d4d4;
                border: 1px solid #3c3c3c;
                border-radius: 4px;
                padding: 6px;
                selection-background-color: #264f78;
            }
            QLineEdit:focus, QTextEdit:focus {
                border: 1px solid #0e639c;
            }
            QComboBox {
                background-color: #2d2d2d;
                color: #d4d4d4;
                border: 1px solid #3c3c3c;
                border-radius: 4px;
                padding: 6px;
            }
            QComboBox:hover {
                border: 1px solid #0e639c;
            }
            QComboBox::drop-down {
                border: none;
                width: 20px;
            }
            QComboBox::down-arrow {
                width: 12px;
                height: 12px;
            }
            QComboBox QAbstractItemView {
                background-color: #2d2d2d;
                color: #d4d4d4;
                selection-background-color: #0e639c;
                border: 1px solid #3c3c3c;
            }
            QListWidget {
                background-color: #2d2d2d;
                color: #d4d4d4;
                border: 1px solid #3c3c3c;
                border-radius: 4px;
            }
            QListWidget::item {
                padding: 6px;
            }
            QListWidget::item:selected {
                background-color: #0e639c;
                color: #ffffff;
            }
            QListWidget::item:hover {
                background-color: #2a2d2e;
            }
            QGroupBox {
                border: 1px solid #3c3c3c;
                border-radius: 6px;
                margin-top: 12px;
                padding-top: 10px;
                font-weight: bold;
                color: #d4d4d4;
            }
            QGroupBox::title {
                subcontrol-origin: margin;
                subcontrol-position: top left;
                left: 10px;
                padding: 0 5px;
                color: #d4d4d4;
            }
            QProgressBar {
                background-color: #2d2d2d;
                border: 1px solid #3c3c3c;
                border-radius: 4px;
                height: 20px;
                text-align: center;
                color: #d4d4d4;
            }
            QProgressBar::chunk {
                background-color: #0e639c;
                border-radius: 3px;
            }
            QScrollArea {
                border: none;
                background-color: #1e1e1e;
            }
            QScrollBar:vertical {
                background-color: #1e1e1e;
                width: 12px;
                border-radius: 6px;
            }
            QScrollBar::handle:vertical {
                background-color: #3c3c3c;
                border-radius: 6px;
                min-height: 30px;
            }
            QScrollBar::handle:vertical:hover {
                background-color: #4c4c4c;
            }
            QScrollBar::add-line:vertical, QScrollBar::sub-line:vertical {
                height: 0;
            }
            QRadioButton {
                color: #d4d4d4;
                spacing: 8px;
            }
            QRadioButton::indicator {
                width: 16px;
                height: 16px;
            }
            QRadioButton::indicator:checked {
                background-color: #0e639c;
                border: 2px solid #0e639c;
                border-radius: 9px;
            }
            QRadioButton::indicator:unchecked {
                background-color: #2d2d2d;
                border: 2px solid #3c3c3c;
                border-radius: 9px;
            }
        """)

    def _setup_extract_widget(self):
        """Setup the extract panel widget."""
        self.extract_widget = QWidget()
        extract_outer_layout = QVBoxLayout(self.extract_widget)

        # Create scroll area for the content
        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        scroll.setFrameShape(QScrollArea.NoFrame)
        scroll_content = QWidget()
        extract_layout = QVBoxLayout(scroll_content)

        # Source Selection Section
        source_group = QGroupBox("Source Selection")
        source_layout = QVBoxLayout()

        # Search input
        self.extract_search_input = QLineEdit()
        self.extract_search_input.setPlaceholderText("Search sources...")
        self.extract_search_input.textChanged.connect(self._filter_extract_sources)
        source_layout.addWidget(self.extract_search_input)

        # Source list
        self.extract_source_list = QListWidget()
        self.extract_source_list.setMaximumHeight(120)
        self.extract_source_list.itemClicked.connect(self._on_extract_source_selected)
        source_layout.addWidget(self.extract_source_list)

        # Refresh button
        self.extract_refresh_button = QPushButton("Refresh Sources")
        self.extract_refresh_button.clicked.connect(self._fetch_extract_sources)
        source_layout.addWidget(self.extract_refresh_button)

        # Selected source label
        self.extract_selected_label = QLabel("No source selected.")
        source_layout.addWidget(self.extract_selected_label)

        source_group.setLayout(source_layout)
        extract_layout.addWidget(source_group)

        # Filter Options Section (optional bbox) - collapsible, starts collapsed
        filter_group = QGroupBox("Filter Options (Optional)")
        filter_group.setCheckable(True)
        filter_group.setChecked(False)

        # Create a container widget for the filter contents
        self.extract_filter_content = QWidget()
        filter_layout = QVBoxLayout(self.extract_filter_content)
        filter_layout.setContentsMargins(0, 0, 0, 0)

        self.extract_bounds_input = QLineEdit()
        self.extract_bounds_input.setPlaceholderText("min_lon,min_lat,max_lon,max_lat (optional)")
        filter_layout.addWidget(QLabel("Bounding Box:"))
        bounds_help_label = QLabel(
            'You can use <a href="http://bboxfinder.com">http://bboxfinder.com</a> to find the bounds.'
        )
        bounds_help_label.setOpenExternalLinks(True)
        filter_layout.addWidget(bounds_help_label)
        filter_layout.addWidget(self.extract_bounds_input)

        filter_group_layout = QVBoxLayout()
        filter_group_layout.addWidget(self.extract_filter_content)
        filter_group.setLayout(filter_group_layout)

        # Connect toggle to show/hide content
        filter_group.toggled.connect(self._toggle_extract_filter)
        self.extract_filter_content.hide()  # Start collapsed

        self.extract_filter_group = filter_group
        extract_layout.addWidget(filter_group)

        # Output Section
        output_group = QGroupBox("Output")
        output_layout = QVBoxLayout()

        self.extract_output_type_combo = QComboBox()
        self.extract_output_type_combo.addItems(
            ["GPKG", "Parquet", "Shapefile", "GeoJSON", "GeoJSONSeq", "FlatGeobuf", "CSV"]
        )
        self.extract_output_type_combo.currentTextChanged.connect(self._update_extract_output_filename)
        output_layout.addWidget(QLabel("Output Type:"))
        output_layout.addWidget(self.extract_output_type_combo)

        self.extract_output_filename_input = QLineEdit("extracted_output.gpkg")
        output_layout.addWidget(QLabel("Output Filename:"))
        output_layout.addWidget(self.extract_output_filename_input)

        output_group.setLayout(output_layout)
        extract_layout.addWidget(output_group)

        # Run Button
        self.extract_run_button = QPushButton("Extract")
        self.extract_run_button.clicked.connect(self._run_extract)
        extract_layout.addWidget(self.extract_run_button)

        # Progress and Status
        self.extract_status_label = QLabel("")
        self.extract_status_label.hide()
        extract_layout.addWidget(self.extract_status_label)

        self.extract_progress_bar = QProgressBar()
        self.extract_progress_bar.hide()
        extract_layout.addWidget(self.extract_progress_bar)

        self.extract_status_text_edit = QTextEdit()
        self.extract_status_text_edit.setReadOnly(True)
        self.extract_status_text_edit.setMaximumHeight(80)
        extract_layout.addWidget(self.extract_status_text_edit)

        # Download Button
        self.extract_download_button = QPushButton("Download Extracted File")
        self.extract_download_button.clicked.connect(self._download_extract_file)
        self.extract_download_button.setEnabled(False)
        extract_layout.addWidget(self.extract_download_button)

        # Add stretch at the bottom to push everything up
        extract_layout.addStretch()

        # Set scroll content and add to outer layout
        scroll.setWidget(scroll_content)
        extract_outer_layout.addWidget(scroll)

        self.content_stack.addWidget(self.extract_widget)

        # Start fetching sources immediately
        self._fetch_extract_sources()

    def _toggle_extract_filter(self, checked):
        """Toggle filter content visibility."""
        self.extract_filter_content.setVisible(checked)

    def _fetch_extract_sources(self):
        """Fetch sources list in background."""
        self.extract_source_list.clear()
        self.extract_source_list.addItem("Loading sources...")
        self.extract_refresh_button.setEnabled(False)

        self.source_fetch_worker = SourceFetchWorker()
        self.source_fetch_worker.finished.connect(self._handle_sources_fetched)
        self.source_fetch_worker.error.connect(self._handle_sources_fetch_error)
        self.source_fetch_worker.start()

    def _handle_sources_fetched(self, sources):
        """Handle fetched sources."""
        self.extract_refresh_button.setEnabled(True)
        self.extract_source_list.clear()

        if sources is None:
            self.extract_source_list.addItem("Failed to fetch sources. Click Refresh to retry.")
            return

        self.extract_sources = sources
        self._populate_source_list(sources)

    def _handle_sources_fetch_error(self, error):
        """Handle source fetch error."""
        self.extract_refresh_button.setEnabled(True)
        self.extract_source_list.clear()
        self.extract_source_list.addItem(f"Error: {error}")

    def _populate_source_list(self, sources):
        """Populate the source list widget."""
        self.extract_source_list.clear()
        for source in sources:
            name = source.get("name", "Unknown")
            item = QListWidgetItem(name)
            item.setData(256, source)  # Store source config in item data (Qt.UserRole = 256)
            self.extract_source_list.addItem(item)

    def _filter_extract_sources(self, search_text):
        """Filter source list by search text."""
        if not self.extract_sources:
            return

        search_lower = search_text.lower().strip()
        if not search_lower:
            self._populate_source_list(self.extract_sources)
            return

        filtered = [
            s
            for s in self.extract_sources
            if search_lower in s.get("name", "").lower() or search_lower in s.get("route", "").lower()
        ]
        self._populate_source_list(filtered)

    def _on_extract_source_selected(self, item):
        """Handle source selection."""
        source = item.data(256)
        if source:
            self.extract_selected_source = source
            self.extract_selected_label.setText(f"Selected: {source.get('name', 'Unknown')}")
            self._update_extract_output_filename()

    def _update_extract_output_filename(self):
        """Update suggested output filename based on source and format."""
        if not self.extract_selected_source:
            return

        source_name = self.extract_selected_source.get("name", "extracted")
        # Sanitize filename
        safe_name = "".join(c if c.isalnum() or c in "-_" else "_" for c in source_name)

        output_type = self.extract_output_type_combo.currentText()
        ext_map = {
            "GPKG": ".gpkg",
            "Parquet": ".parquet",
            "Shapefile": ".shp",
            "GeoJSON": ".geojson",
            "GeoJSONSeq": ".geojsonl",
            "FlatGeobuf": ".fgb",
            "CSV": ".csv",
        }
        ext = ext_map.get(output_type, ".gpkg")
        self.extract_output_filename_input.setText(f"{safe_name}{ext}")

    def _run_extract(self):
        """Run the extract operation."""
        self.extract_status_text_edit.clear()
        self.extract_download_button.setEnabled(False)

        if not self.extract_selected_source:
            self._log_status(
                "Error: Please select a source.",
                level="error",
                target_text_edit=self.extract_status_text_edit,
            )
            return

        # Validate bounds if filter group is checked and bounds provided
        bounds = None
        if self.extract_filter_group.isChecked():
            bounds = self.extract_bounds_input.text().strip()
            if bounds:
                parts = bounds.split(",")
                if len(parts) != 4:
                    self._log_status(
                        "Error: Bounds must be in format: min_lon,min_lat,max_lon,max_lat",
                        level="error",
                        target_text_edit=self.extract_status_text_edit,
                    )
                    return
                try:
                    [float(p) for p in parts]
                except ValueError:
                    self._log_status(
                        "Error: Bounds must contain numeric values.",
                        level="error",
                        target_text_edit=self.extract_status_text_edit,
                    )
                    return

        output_type = self.extract_output_type_combo.currentText()
        output_filename = self.extract_output_filename_input.text()

        if not output_filename:
            self._log_status(
                "Error: Please enter an output filename.",
                level="error",
                target_text_edit=self.extract_status_text_edit,
            )
            return

        # Map UI output type to driver name
        driver_map = {
            "GPKG": "GPKG",
            "Parquet": "Parquet",
            "Shapefile": "ESRI Shapefile",
            "GeoJSON": "GeoJSON",
            "GeoJSONSeq": "GeoJSONSeq",
            "FlatGeobuf": "FlatGeobuf",
            "CSV": "CSV",
        }
        output_driver = driver_map.get(output_type, "GPKG")

        # Create temp directory for output
        self.temp_dir_extract_obj = tempfile.TemporaryDirectory(delete=False, ignore_cleanup_errors=True)
        temp_dir = Path(self.temp_dir_extract_obj.name)
        self.extract_output_file_path = temp_dir / output_filename

        self._log_status(
            f"Extracting from {self.extract_selected_source.get('name', 'Unknown')}...",
            target_text_edit=self.extract_status_text_edit,
        )

        # Show progress
        self.extract_status_label.setText("Starting extraction...")
        self.extract_status_label.show()
        self.extract_progress_bar.setRange(0, 0)  # Indeterminate
        self.extract_progress_bar.show()
        self.extract_run_button.setEnabled(False)

        # Start worker
        self.extract_worker = ExtractWorker(
            source_config=self.extract_selected_source,
            bounds=bounds if bounds else None,
            output_file_path=self.extract_output_file_path,
            output_driver=output_driver,
        )
        self.extract_worker.status.connect(self._update_extract_status)
        self.extract_worker.finished.connect(self._handle_extract_finished)
        self.extract_worker.error.connect(self._handle_extract_error)
        self.extract_worker.start()

    def _update_extract_status(self, status):
        """Update extract status label."""
        self.extract_status_label.setText(status)

    def _handle_extract_finished(self, success):
        """Handle extract completion."""
        self.extract_status_label.hide()
        self.extract_progress_bar.hide()
        self.extract_run_button.setEnabled(True)

        if success:
            self._log_status(
                "Extraction complete! You can download the output file below.",
                target_text_edit=self.extract_status_text_edit,
            )
            self.extract_download_button.setEnabled(True)
        else:
            self._log_status(
                "Extraction failed.",
                level="error",
                target_text_edit=self.extract_status_text_edit,
            )

    def _handle_extract_error(self, error_message):
        """Handle extract error."""
        self.extract_status_label.hide()
        self.extract_progress_bar.hide()
        self.extract_run_button.setEnabled(True)
        self._log_status(
            f"Error during extraction: {error_message}",
            level="error",
            target_text_edit=self.extract_status_text_edit,
        )

    def _download_extract_file(self):
        """Download the extracted file."""
        if self.extract_output_file_path and self.extract_output_file_path.exists():
            file_dialog = QFileDialog()
            # Use the filename from the stored path (captured at extraction time)
            save_path, _ = file_dialog.getSaveFileName(
                self,
                "Save Extracted File",
                self.extract_output_file_path.name,
                "All Files (*)",
            )
            if save_path:
                try:
                    # Handle shapefile with multiple components
                    if self.extract_output_file_path.suffix.lower() == ".shp":
                        moved_files = self._move_shapefile_components(self.extract_output_file_path, Path(save_path))
                        self._log_status(
                            f"Shapefile saved to: {save_path} ({len(moved_files)} files)",
                            target_text_edit=self.extract_status_text_edit,
                        )
                    else:
                        shutil.move(str(self.extract_output_file_path), save_path)
                        self._log_status(
                            f"File saved to: {save_path}",
                            target_text_edit=self.extract_status_text_edit,
                        )
                    self.extract_output_file_path = None
                    self.extract_download_button.setEnabled(False)
                except Exception as e:
                    self._log_status(
                        f"Error saving file: {e}",
                        level="error",
                        target_text_edit=self.extract_status_text_edit,
                    )
        else:
            self._log_status(
                "No extracted file to download.",
                level="warning",
                target_text_edit=self.extract_status_text_edit,
            )

        self._clear_temp_files_extract()

    def _clear_temp_files_extract(self):
        """Clean up extract temp files."""
        if self.temp_dir_extract_obj:
            self.temp_dir_extract_obj.cleanup()
            self.temp_dir_extract_obj = None

    def _move_shapefile_components(self, source_path: Path, dest_path: Path):
        """Move all shapefile components (.shp, .shx, .dbf, .prj, .cpg, etc.) to destination."""
        source_stem = source_path.stem
        dest_stem = Path(dest_path).stem
        dest_dir = Path(dest_path).parent

        # Shapefile auxiliary extensions
        shp_extensions = [
            ".shp",
            ".shx",
            ".dbf",
            ".prj",
            ".cpg",
            ".sbn",
            ".sbx",
            ".fbn",
            ".fbx",
            ".ain",
            ".aih",
            ".xml",
        ]

        moved_files = []
        for ext in shp_extensions:
            src_file = source_path.parent / f"{source_stem}{ext}"
            if src_file.exists():
                dest_file = dest_dir / f"{dest_stem}{ext}"
                shutil.move(str(src_file), str(dest_file))
                moved_files.append(dest_file.name)

        return moved_files

    def _log_status(self, message, level="info", target_text_edit=None):
        if target_text_edit is None:
            target_text_edit = self.extract_status_text_edit

        if level == "error":
            target_text_edit.append(f"<p style='color:red;'>{message}</p>")
        elif level == "warning":
            target_text_edit.append(f"<p style='color:orange;'>{message}</p>")
        else:
            target_text_edit.append(f"<p style='color:blue;'>{message}</p>")
        QApplication.processEvents()  # Update UI immediately

    def closeEvent(self, event):
        self._clear_temp_files_extract()
        super().closeEvent(event)


def main():
    app = QApplication(sys.argv)
    window = PyQtApp()
    window.show()
    sys.exit(app.exec_())


@click.command("ui")
def ui():
    """Launch the PyQt UI for Indianopenmaps"""
    main()


if __name__ == "__main__":
    main()
