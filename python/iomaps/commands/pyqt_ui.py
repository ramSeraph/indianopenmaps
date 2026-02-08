import json
import logging
import shutil
import sys
import tempfile
from pathlib import Path

import click
import fiona
import multivolumefile
import py7zr
from fiona.crs import CRS
from fiona.model import to_dict
from PyQt5.QtCore import QThread, pyqtSignal
from PyQt5.QtWidgets import (
    QApplication,
    QButtonGroup,
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
    QRadioButton,
    QScrollArea,
    QStackedWidget,
    QTextEdit,
    QVBoxLayout,
    QWidget,
)

from iomaps.commands.filter_7z import (
    create_writer,
    process_archive,
)
from iomaps.commands.schema_common import get_schema_from_archive
from iomaps.core.helpers import get_geojsonl_file_info, get_supported_input_drivers
from iomaps.core.spatial_filter import PassThroughFilter, create_filter


class SchemaInferenceWorker(QThread):
    progress = pyqtSignal(int)
    finished = pyqtSignal(object)
    error = pyqtSignal(str)

    def __init__(self, input_7z_path):
        super().__init__()
        self.input_7z_path = input_7z_path

    def run(self):
        try:
            schema, renames = get_schema_from_archive(
                self.input_7z_path,
                external_updater=self.progress,
            )
            self.finished.emit((schema, renames))
        except Exception as e:
            self.error.emit(str(e))


class Filter7zWorker(QThread):
    progress = pyqtSignal(int)
    finished = pyqtSignal(object)
    error = pyqtSignal(str)

    def __init__(self, input_7z_path, filter_args, schema, output_file_path, output_driver, property_renames=None):
        super().__init__()
        self.input_7z_path = input_7z_path
        self.filter_args = filter_args
        self.schema = schema
        self.output_file_path = output_file_path
        self.output_driver = output_driver
        self.property_renames = property_renames or {}

    def run(self):
        try:
            # Create filter or use PassThroughFilter if no filter specified
            schema_properties = self.schema.get("properties", {})
            if self.filter_args.get("filter_file") or self.filter_args.get("bounds"):
                filter_obj = create_filter(
                    **self.filter_args,
                    property_renames=self.property_renames,
                    schema_properties=schema_properties,
                )
                if filter_obj is None:
                    raise Exception("Failed to create filter.")
            else:
                filter_obj = PassThroughFilter(
                    property_renames=self.property_renames,
                    schema_properties=schema_properties,
                )

            crs = CRS.from_epsg(4326)
            writer = create_writer(str(self.output_file_path), self.schema, crs, self.output_driver)

            class FilterProgressUpdater:
                def __init__(self, signal, total_size):
                    self.signal = signal
                    self.total_size = total_size
                    self.processed_size = 0

                def update_size(self, sz):
                    self.processed_size += sz
                    if self.total_size > 0:
                        percentage = int((self.processed_size / self.total_size) * 100)
                        self.signal.emit(percentage)

                def update_other_info(self, **kwargs):
                    # This method is called by TQDMUpdater, but we don't need to do anything with it here
                    pass

            # Get total file size for progress bar
            archive_file = py7zr.SevenZipFile(str(self.input_7z_path), mode="r")
            target_file_info = get_geojsonl_file_info(archive_file)
            archive_file.close()  # Close the archive after getting info

            total_size = 0
            if target_file_info:
                total_size = target_file_info.uncompressed

            updater = FilterProgressUpdater(self.progress, total_size)

            if str(self.input_7z_path).endswith(".001"):
                base_path = str(self.input_7z_path).rsplit(".", 1)[0]
                with multivolumefile.open(base_path, "rb") as multivolume_file:
                    with py7zr.SevenZipFile(multivolume_file, "r") as archive:
                        process_archive(archive, filter_obj, writer, external_updater=updater)
            else:
                with py7zr.SevenZipFile(str(self.input_7z_path), mode="r") as archive:
                    process_archive(archive, filter_obj, writer, external_updater=updater)
            self.finished.emit(True)
        except Exception as e:
            self.error.emit(str(e))


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

        self.sidebar_layout = QVBoxLayout()
        self.content_stack = QStackedWidget()  # Use QStackedWidget to switch between views

        self.main_layout.addLayout(self.sidebar_layout, 1)
        self.main_layout.addWidget(self.content_stack, 3)

        self.temp_dir_filter_7z_obj = None
        self.temp_dir_infer_schema_obj = None
        self.temp_dir_extract_obj = None

        # Internal state variables for Filter 7z
        self.filter_7z_input_7z_path = None
        self.filter_7z_filter_file_path = None
        self.filter_7z_schema_file_path = None
        self.filter_7z_output_file_path = None
        self.filter_7z_suggested_output_filename = "filtered_output.gpkg"
        self.filter_7z_features = []

        # Internal state variables for Infer Schema
        self.infer_schema_input_7z_path = None
        self.infer_schema_output_file_path = None  # Path where the inferred schema will be saved temporarily

        # Internal state variables for Extract
        self.extract_sources = []  # Full list of sources
        self.extract_selected_source = None
        self.extract_output_file_path = None

        self._setup_ui()

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

    def _setup_ui(self):
        # Sidebar for tool selection
        self.extract_button = QPushButton("Extract")
        self.extract_button.clicked.connect(lambda: self._show_tool_view("extract"))
        self.filter_7z_button = QPushButton("Filter 7z")
        self.filter_7z_button.clicked.connect(lambda: self._show_tool_view("filter_7z"))
        self.infer_schema_button = QPushButton("Infer Schema")
        self.infer_schema_button.clicked.connect(lambda: self._show_tool_view("infer_schema"))

        self.sidebar_layout.addWidget(self.extract_button)
        self.sidebar_layout.addWidget(self.filter_7z_button)
        self.sidebar_layout.addWidget(self.infer_schema_button)
        self.sidebar_layout.addStretch()  # Push buttons to the top

        # --- Extract Widget ---
        self._setup_extract_widget()

        # --- Filter 7z Widget ---
        self.filter_7z_widget = QWidget()
        filter_7z_outer_layout = QVBoxLayout(self.filter_7z_widget)

        # Create scroll area for all content
        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        scroll.setFrameShape(QScrollArea.NoFrame)
        scroll_content = QWidget()
        filter_7z_scroll_layout = QVBoxLayout(scroll_content)

        # Input/Output Section for Filter 7z
        filter_7z_input_output_group = QGroupBox("Input/Output")
        filter_7z_input_output_layout = QVBoxLayout()

        self.filter_7z_upload_7z_button = QPushButton("Upload 7z Archive")
        self.filter_7z_upload_7z_button.clicked.connect(self._upload_7z_archive)
        self.filter_7z_input_7z_label = QLabel("No 7z archive selected.")
        filter_7z_input_output_layout.addWidget(self.filter_7z_upload_7z_button)
        filter_7z_input_output_layout.addWidget(self.filter_7z_input_7z_label)

        self.filter_7z_output_type_combo = QComboBox()
        self.filter_7z_output_type_combo.addItems(["GPKG", "Shapefile", "GeoJSON", "GeoJSONSeq", "FlatGeobuf", "CSV"])
        self.filter_7z_output_type_combo.currentTextChanged.connect(self._update_suggested_output_filename)
        filter_7z_input_output_layout.addWidget(QLabel("Output Type:"))
        filter_7z_input_output_layout.addWidget(self.filter_7z_output_type_combo)

        self.filter_7z_output_filename_input = QLineEdit("filtered_output.gpkg")
        filter_7z_input_output_layout.addWidget(QLabel("Suggested Output Filename:"))
        filter_7z_input_output_layout.addWidget(self.filter_7z_output_filename_input)

        filter_7z_input_output_group.setLayout(filter_7z_input_output_layout)
        filter_7z_scroll_layout.addWidget(filter_7z_input_output_group)

        # Filtering Options Section for Filter 7z - collapsible, starts collapsed
        filter_7z_filtering_options_group = QGroupBox("Filtering Options (Optional)")
        filter_7z_filtering_options_group.setCheckable(True)
        filter_7z_filtering_options_group.setChecked(False)

        # Container for filter options content
        self.filter_7z_filtering_content = QWidget()
        filter_7z_filtering_options_layout = QVBoxLayout(self.filter_7z_filtering_content)
        filter_7z_filtering_options_layout.setContentsMargins(0, 0, 0, 0)

        self.filter_7z_filter_type_group = QButtonGroup(self)
        self.filter_7z_filter_file_radio = QRadioButton("Filter File")
        self.filter_7z_bounds_radio = QRadioButton("Bounds")
        self.filter_7z_filter_type_group.addButton(self.filter_7z_filter_file_radio)
        self.filter_7z_filter_type_group.addButton(self.filter_7z_bounds_radio)
        self.filter_7z_filter_file_radio.setChecked(True)  # Default selection

        filter_7z_filtering_options_layout.addWidget(self.filter_7z_filter_file_radio)
        filter_7z_filtering_options_layout.addWidget(self.filter_7z_bounds_radio)

        self.filter_7z_filter_file_widgets = QWidget()
        filter_7z_filter_file_layout = QVBoxLayout(self.filter_7z_filter_file_widgets)
        self.filter_7z_upload_filter_file_button = QPushButton(
            "Upload Filter File (e.g., Shapefile, GeoJSON, FlatGeobuf)"
        )
        self.filter_7z_upload_filter_file_button.clicked.connect(self._upload_filter_file)
        self.filter_7z_filter_file_label = QLabel("No filter file selected.")
        self.filter_7z_filter_file_driver_combo = QComboBox()
        self.filter_7z_filter_file_driver_combo.addItem("Infer from extension")
        self.filter_7z_filter_file_driver_combo.addItems(get_supported_input_drivers())
        filter_7z_filter_file_layout.addWidget(self.filter_7z_upload_filter_file_button)
        filter_7z_filter_file_layout.addWidget(self.filter_7z_filter_file_label)
        self.filter_7z_feature_label = QLabel("Select Feature:")
        self.filter_7z_feature_combo = QComboBox()
        self.filter_7z_feature_label.hide()
        self.filter_7z_feature_combo.hide()
        filter_7z_filter_file_layout.addWidget(self.filter_7z_feature_label)
        filter_7z_filter_file_layout.addWidget(self.filter_7z_feature_combo)
        filter_7z_filter_file_layout.addWidget(QLabel("Filter File Driver:"))
        filter_7z_filter_file_layout.addWidget(self.filter_7z_filter_file_driver_combo)
        filter_7z_filtering_options_layout.addWidget(self.filter_7z_filter_file_widgets)

        self.filter_7z_bounds_widgets = QWidget()
        filter_7z_bounds_layout = QVBoxLayout(self.filter_7z_bounds_widgets)
        self.filter_7z_bounds_input = QLineEdit()
        self.filter_7z_bounds_input.setPlaceholderText("min_lon,min_lat,max_lon,max_lat")
        filter_7z_bounds_layout.addWidget(QLabel("Enter Bounds:"))
        bounds_help_label = QLabel(
            'You can use <a href="http://bboxfinder.com">http://bboxfinder.com</a> to find the bounds.'
        )
        bounds_help_label.setOpenExternalLinks(True)
        filter_7z_bounds_layout.addWidget(bounds_help_label)
        filter_7z_bounds_layout.addWidget(self.filter_7z_bounds_input)
        filter_7z_filtering_options_layout.addWidget(self.filter_7z_bounds_widgets)
        self.filter_7z_bounds_widgets.hide()  # Hide by default

        self.filter_7z_filter_file_radio.toggled.connect(self._toggle_filter_type_widgets)
        self.filter_7z_bounds_radio.toggled.connect(self._toggle_filter_type_widgets)

        filter_7z_filtering_group_layout = QVBoxLayout()
        filter_7z_filtering_group_layout.addWidget(self.filter_7z_filtering_content)
        filter_7z_filtering_options_group.setLayout(filter_7z_filtering_group_layout)
        filter_7z_filtering_options_group.toggled.connect(self._toggle_filter_7z_filtering_options)
        self.filter_7z_filtering_content.hide()  # Start collapsed
        self.filter_7z_filtering_options_group = filter_7z_filtering_options_group
        filter_7z_scroll_layout.addWidget(filter_7z_filtering_options_group)

        # Advanced Options Section for Filter 7z - collapsible, starts collapsed
        filter_7z_advanced_options_group = QGroupBox("Advanced Options")
        filter_7z_advanced_options_group.setCheckable(True)
        filter_7z_advanced_options_group.setChecked(False)

        # Container for advanced options content
        self.filter_7z_advanced_content = QWidget()
        filter_7z_advanced_options_layout = QVBoxLayout(self.filter_7z_advanced_content)
        filter_7z_advanced_options_layout.setContentsMargins(0, 0, 0, 0)

        # Schema Section for Filter 7z
        filter_7z_schema_group = QGroupBox("Schema")
        filter_7z_schema_layout = QVBoxLayout()

        self.filter_7z_upload_schema_button = QPushButton("Upload Schema File (JSON)")
        self.filter_7z_upload_schema_button.clicked.connect(self._upload_schema_file)
        self.filter_7z_upload_schema_button.setToolTip(
            "Upload a JSON schema file to define the expected structure and types of features in the output. "
            "If not provided, an extra pass is done over the data to infer the schema automatically."
        )
        self.filter_7z_schema_file_label = QLabel("No schema file selected.")
        filter_7z_schema_layout.addWidget(self.filter_7z_upload_schema_button)
        filter_7z_schema_layout.addWidget(self.filter_7z_schema_file_label)
        filter_7z_schema_group.setLayout(filter_7z_schema_layout)

        filter_7z_advanced_options_layout.addWidget(filter_7z_schema_group)

        self.filter_7z_log_level_combo = QComboBox()
        self.filter_7z_log_level_combo.addItems(["INFO", "DEBUG", "WARNING", "ERROR", "CRITICAL"])
        filter_7z_advanced_options_layout.addWidget(QLabel("Log Level:"))
        filter_7z_advanced_options_layout.addWidget(self.filter_7z_log_level_combo)

        filter_7z_advanced_group_layout = QVBoxLayout()
        filter_7z_advanced_group_layout.addWidget(self.filter_7z_advanced_content)
        filter_7z_advanced_options_group.setLayout(filter_7z_advanced_group_layout)
        filter_7z_advanced_options_group.toggled.connect(self._toggle_filter_7z_advanced_options)
        self.filter_7z_advanced_content.hide()  # Start collapsed
        self.filter_7z_advanced_options_group = filter_7z_advanced_options_group
        filter_7z_scroll_layout.addWidget(filter_7z_advanced_options_group)

        # Main Content - Run Filter Button and Output for Filter 7z
        self.filter_7z_run_filter_button = QPushButton("Run Filter")
        self.filter_7z_run_filter_button.clicked.connect(self._run_filter)
        filter_7z_scroll_layout.addWidget(self.filter_7z_run_filter_button)

        self.filter_7z_schema_inference_label = QLabel("Schema Inference Progress:")
        self.filter_7z_schema_inference_label.hide()
        self.filter_7z_schema_inference_progress_bar = QProgressBar()
        self.filter_7z_schema_inference_progress_bar.hide()
        filter_7z_scroll_layout.addWidget(self.filter_7z_schema_inference_label)
        filter_7z_scroll_layout.addWidget(self.filter_7z_schema_inference_progress_bar)

        self.filter_7z_filter_label = QLabel("Filtering Progress:")
        self.filter_7z_filter_label.hide()
        self.filter_7z_filter_progress_bar = QProgressBar()
        self.filter_7z_filter_progress_bar.hide()
        filter_7z_scroll_layout.addWidget(self.filter_7z_filter_label)
        filter_7z_scroll_layout.addWidget(self.filter_7z_filter_progress_bar)

        self.filter_7z_status_text_edit = QTextEdit()
        self.filter_7z_status_text_edit.setReadOnly(True)
        self.filter_7z_status_text_edit.setMaximumHeight(80)
        filter_7z_scroll_layout.addWidget(self.filter_7z_status_text_edit)

        self.filter_7z_download_button = QPushButton("Download Filtered File")
        self.filter_7z_download_button.clicked.connect(self._download_file)
        self.filter_7z_download_button.setEnabled(False)  # Disabled until filtering is complete
        filter_7z_scroll_layout.addWidget(self.filter_7z_download_button)

        # Add stretch at the bottom
        filter_7z_scroll_layout.addStretch()

        # Set scroll content and add to outer layout
        scroll.setWidget(scroll_content)
        filter_7z_outer_layout.addWidget(scroll)

        self.content_stack.addWidget(self.filter_7z_widget)  # Add filter_7z_widget to the stack

        # --- Infer Schema Widget ---
        self.infer_schema_widget = QWidget()
        infer_schema_layout = QVBoxLayout(self.infer_schema_widget)

        # Input Section for Infer Schema
        infer_schema_input_group = QGroupBox("Input")
        infer_schema_input_layout = QVBoxLayout()

        self.infer_schema_upload_7z_button = QPushButton("Upload 7z Archive")
        self.infer_schema_upload_7z_button.clicked.connect(self._upload_infer_schema_7z_archive)
        self.infer_schema_input_7z_label = QLabel("No 7z archive selected.")
        infer_schema_input_layout.addWidget(self.infer_schema_upload_7z_button)
        infer_schema_input_layout.addWidget(self.infer_schema_input_7z_label)

        infer_schema_input_group.setLayout(infer_schema_input_layout)
        infer_schema_layout.addWidget(infer_schema_input_group)

        # Options Section for Infer Schema
        infer_schema_options_group = QGroupBox("Options")
        infer_schema_options_layout = QVBoxLayout()

        self.infer_schema_log_level_combo = QComboBox()
        self.infer_schema_log_level_combo.addItems(["INFO", "DEBUG", "WARNING", "ERROR", "CRITICAL"])
        infer_schema_options_layout.addWidget(QLabel("Log Level:"))
        infer_schema_options_layout.addWidget(self.infer_schema_log_level_combo)

        infer_schema_options_group.setLayout(infer_schema_options_layout)
        infer_schema_layout.addWidget(infer_schema_options_group)

        # Run Button and Output for Infer Schema
        self.infer_schema_run_button = QPushButton("Infer Schema")
        self.infer_schema_run_button.clicked.connect(self._run_infer_schema)
        infer_schema_layout.addWidget(self.infer_schema_run_button)

        self.infer_schema_progress_bar = QProgressBar()
        self.infer_schema_progress_bar.hide()
        infer_schema_layout.addWidget(self.infer_schema_progress_bar)

        self.infer_schema_output_text_edit = QTextEdit()
        self.infer_schema_output_text_edit.setReadOnly(True)
        self.infer_schema_output_text_edit.setMaximumHeight(150)  # Set a maximum height
        infer_schema_layout.addWidget(self.infer_schema_output_text_edit)

        # Download Options for Infer Schema
        infer_schema_download_group = QGroupBox("Download Options")
        infer_schema_download_layout = QVBoxLayout()

        self.infer_schema_output_file_input = QLineEdit()
        self.infer_schema_output_file_input.setPlaceholderText(
            "Optional: Path for output schema file (e.g., schema.json)"
        )
        infer_schema_download_layout.addWidget(QLabel("Output Schema File:"))
        infer_schema_download_layout.addWidget(self.infer_schema_output_file_input)

        self.infer_schema_download_button = QPushButton("Download Schema File")
        self.infer_schema_download_button.clicked.connect(self._download_inferred_schema)
        self.infer_schema_download_button.setEnabled(False)
        infer_schema_download_layout.addWidget(self.infer_schema_download_button)

        infer_schema_download_group.setLayout(infer_schema_download_layout)
        infer_schema_layout.addWidget(infer_schema_download_group)
        infer_schema_layout.addStretch()  # Add stretch to prevent download group from stretching

        self.content_stack.addWidget(self.infer_schema_widget)  # Add infer_schema_widget to the stack

        # Set initial view
        self._show_tool_view("extract")

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

    def _show_tool_view(self, tool_name):
        if tool_name == "extract":
            self.content_stack.setCurrentWidget(self.extract_widget)
            self.setWindowTitle("Indianopenmaps - Extract")
        elif tool_name == "filter_7z":
            self.content_stack.setCurrentWidget(self.filter_7z_widget)
            self.setWindowTitle("Indianopenmaps - Filter 7z")
        elif tool_name == "infer_schema":
            self.content_stack.setCurrentWidget(self.infer_schema_widget)
            self.setWindowTitle("Indianopenmaps - Infer Schema")

    def _toggle_filter_7z_filtering_options(self, checked):
        """Toggle filter options content visibility."""
        self.filter_7z_filtering_content.setVisible(checked)

    def _toggle_filter_7z_advanced_options(self, checked):
        """Toggle advanced options content visibility."""
        self.filter_7z_advanced_content.setVisible(checked)

    def _toggle_filter_type_widgets(self):
        if self.filter_7z_filter_file_radio.isChecked():
            self.filter_7z_filter_file_widgets.show()
            self.filter_7z_bounds_widgets.hide()
        else:
            self.filter_7z_filter_file_widgets.hide()
            self.filter_7z_bounds_widgets.show()

    def _update_suggested_output_filename(self):
        if not self.filter_7z_input_7z_path:
            return
        base_name = self.filter_7z_input_7z_path.stem
        output_type = self.filter_7z_output_type_combo.currentText()
        if output_type == "GPKG":
            self.filter_7z_output_filename_input.setText(f"{base_name}.filtered.gpkg")
        elif output_type == "Shapefile":
            self.filter_7z_output_filename_input.setText(f"{base_name}.filtered.shp")
        elif output_type == "GeoJSON":
            self.filter_7z_output_filename_input.setText(f"{base_name}.filtered.geojson")
        elif output_type == "GeoJSONSeq":
            self.filter_7z_output_filename_input.setText(f"{base_name}.filtered.geojsonl")
        elif output_type == "FlatGeobuf":
            self.filter_7z_output_filename_input.setText(f"{base_name}.filtered.fgb")
        elif output_type == "CSV":
            self.filter_7z_output_filename_input.setText(f"{base_name}.filtered.csv")

    def _upload_7z_archive(self):
        file_dialog = QFileDialog()
        file_path, _ = file_dialog.getOpenFileName(self, "Upload 7z Archive", "", "7z Archives (*.7z)")
        if file_path:
            self.filter_7z_input_7z_path = Path(file_path)
            self.filter_7z_input_7z_label.setText(f"Selected: {self.filter_7z_input_7z_path.name}")
            self._update_suggested_output_filename()
        else:
            self.filter_7z_input_7z_path = None
            self.filter_7z_input_7z_label.setText("No 7z archive selected.")
            self.filter_7z_output_filename_input.setText("filtered_output.gpkg")

    def _upload_filter_file(self):
        file_dialog = QFileDialog()
        file_path, _ = file_dialog.getOpenFileName(
            self,
            "Upload Filter File",
            "",
            "Filter Files (*.shp *.geojson *.json *.geojsonl *.fgb)",
        )
        self.filter_7z_feature_combo.clear()
        self.filter_7z_features = []
        self.filter_7z_feature_label.hide()
        self.filter_7z_feature_combo.hide()
        if file_path:
            self.filter_7z_filter_file_path = Path(file_path)
            self.filter_7z_filter_file_label.setText(f"Selected: {self.filter_7z_filter_file_path.name}")
            count = 0
            try:
                with fiona.open(file_path, "r") as collection:
                    for feature in collection:
                        # if gemotry type is not Polygon, MultiPolygon, skip
                        if feature["geometry"]["type"] not in [
                            "Polygon",
                            "MultiPolygon",
                        ]:
                            continue
                        self.filter_7z_features.append(feature)
                        # Create a display text for the dropdown
                        properties = None
                        if "properties" in feature:
                            properties = ", ".join([f"{k}: {v}" for k, v in feature["properties"].items()])
                        display_text = f"Feature {count} - {properties}"
                        self.filter_7z_feature_combo.addItem(display_text)
                        count += 1
                if len(self.filter_7z_features) > 1:
                    self.filter_7z_feature_label.show()
                    self.filter_7z_feature_combo.show()
            except Exception as e:
                self._log_status(
                    f"Error reading filter file: {e}",
                    level="error",
                    target_text_edit=self.filter_7z_status_text_edit,
                )
                self.filter_7z_filter_file_path = None
                self.filter_7z_filter_file_label.setText("No filter file selected.")
        else:
            self.filter_7z_filter_file_path = None
            self.filter_7z_filter_file_label.setText("No filter file selected.")

    def _upload_schema_file(self):
        file_dialog = QFileDialog()
        file_path, _ = file_dialog.getOpenFileName(self, "Upload Schema File", "", "JSON Files (*.json)")
        if file_path:
            self.filter_7z_schema_file_path = Path(file_path)
            self.filter_7z_schema_file_label.setText(f"Selected: {self.filter_7z_schema_file_path.name}")
        else:
            self.filter_7z_schema_file_path = None
            self.filter_7z_schema_file_label.setText("No schema file selected.")

    def _run_filter(self):
        self.filter_7z_status_text_edit.clear()
        self.filter_7z_download_button.setEnabled(False)

        if not self.filter_7z_input_7z_path:
            self._log_status(
                "Error: Please upload an input 7z archive.",
                level="error",
                target_text_edit=self.filter_7z_status_text_edit,
            )
            return

        # Check if filtering options are enabled
        filtering_enabled = self.filter_7z_filtering_options_group.isChecked()

        if filtering_enabled:
            filter_type = "Filter File" if self.filter_7z_filter_file_radio.isChecked() else "Bounds"
            if filter_type == "Filter File" and not self.filter_7z_filter_file_path:
                self._log_status(
                    "Error: Please upload a valid filter file.",
                    level="error",
                    target_text_edit=self.filter_7z_status_text_edit,
                )
                return
            elif filter_type == "Bounds" and not self.filter_7z_bounds_input.text():
                self._log_status(
                    "Error: Please enter bounds.",
                    level="error",
                    target_text_edit=self.filter_7z_status_text_edit,
                )
                return
        else:
            filter_type = None

        # Setup logging
        log_level = self.filter_7z_log_level_combo.currentText()
        logging.basicConfig(
            level=getattr(logging, log_level.upper()),
            format="%(asctime)s - %(levelname)s - %(message)s",
        )
        self._log_status(
            f"Processing {self.filter_7z_input_7z_path.name}",
            target_text_edit=self.filter_7z_status_text_edit,
        )

        schema_path_for_processing = None
        if self.filter_7z_advanced_options_group.isChecked() and self.filter_7z_schema_file_path:
            schema_path_for_processing = self.filter_7z_schema_file_path

        # run this in the context of a temporary directory
        self.temp_dir_filter_7z_obj = tempfile.TemporaryDirectory(delete=False, ignore_cleanup_errors=True)
        temp_dir_filter_7z = Path(self.temp_dir_filter_7z_obj.name)

        filter_file_for_processing = self.filter_7z_filter_file_path if filtering_enabled else None
        if filtering_enabled and len(self.filter_7z_features) > 1:
            selected_index = self.filter_7z_feature_combo.currentIndex()
            selected_feature = self.filter_7z_features[selected_index]

            # Write the selected feature to a temporary file
            temp_filter_file = temp_dir_filter_7z / "temp_filter.geojson"

            # Create a GeoJSON FeatureCollection with a single feature
            feature_collection = {
                "type": "FeatureCollection",
                "features": [to_dict(selected_feature)],
            }
            temp_filter_file.write_text(json.dumps(feature_collection))
            filter_file_for_processing = temp_filter_file

        # Prepare arguments for create_filter
        if filtering_enabled:
            self.filter_args = {
                "filter_file": str(filter_file_for_processing)
                if filter_file_for_processing and filter_type == "Filter File"
                else None,
                "filter_file_driver": self.filter_7z_filter_file_driver_combo.currentText()
                if self.filter_7z_filter_file_driver_combo.currentText() != "Infer from extension"
                else None,
                "bounds": self.filter_7z_bounds_input.text() if filter_type == "Bounds" else None,
            }
        else:
            # No filtering - pass through all features
            self.filter_args = {
                "filter_file": None,
                "filter_file_driver": None,
                "bounds": None,
            }

        # Prepare schema
        if schema_path_for_processing:
            self._log_status(
                f"Reading schema from {schema_path_for_processing}",
                target_text_edit=self.filter_7z_status_text_edit,
            )
            with schema_path_for_processing.open("r") as f:
                schema = json.load(f)
            self._start_filter_process(schema)
        else:
            self._log_status(
                f"Inferring schema from {self.filter_7z_input_7z_path}",
                target_text_edit=self.filter_7z_status_text_edit,
            )
            self.filter_7z_schema_inference_label.show()
            self.filter_7z_schema_inference_progress_bar.show()
            self.filter_7z_schema_inference_progress_bar.setValue(0)
            self.schema_inference_worker = SchemaInferenceWorker(str(self.filter_7z_input_7z_path))
            self.schema_inference_worker.progress.connect(self._update_filter_7z_schema_inference_progress)
            self.schema_inference_worker.finished.connect(self._handle_filter_7z_schema_inference_finished)
            self.schema_inference_worker.error.connect(self._handle_filter_7z_schema_inference_error)
            self.schema_inference_worker.start()

    def _start_filter_process(self, schema, property_renames=None):
        if schema is None:
            self._log_status(
                "Error: Schema is not available for filtering.",
                level="error",
                target_text_edit=self.filter_7z_status_text_edit,
            )
            return

        property_renames = property_renames or {}

        output_type = self.filter_7z_output_type_combo.currentText()

        # Check for shapefile with mixed geometry types
        if output_type.lower() == "esri shapefile":
            geom_types = schema.get("geometry", [])
            if isinstance(geom_types, list) and len(geom_types) > 1:
                # Check if types are incompatible (not just single/multi variants)
                base_types = set()
                for gt in geom_types:
                    if gt.startswith("Multi"):
                        base_types.add(gt[5:])  # Remove "Multi" prefix
                    else:
                        base_types.add(gt)
                if len(base_types) > 1:
                    self._log_status(
                        f"Error: Shapefile does not support mixed geometry types. "
                        f"Schema has: {', '.join(geom_types)}. "
                        f"Please choose a different output format (e.g., GeoPackage, FlatGeobuf).",
                        level="error",
                        target_text_edit=self.filter_7z_status_text_edit,
                    )
                    return

        self._log_status(
            "Starting filtering process...",
            target_text_edit=self.filter_7z_status_text_edit,
        )
        self.filter_7z_filter_label.show()
        self.filter_7z_filter_progress_bar.show()
        self.filter_7z_filter_progress_bar.setValue(0)

        temp_dir_filter_7z = Path(self.temp_dir_filter_7z_obj.name)
        self.filter_7z_output_file_path = temp_dir_filter_7z / self.filter_7z_output_filename_input.text()
        output_driver = output_type

        self.filter_worker = Filter7zWorker(
            str(self.filter_7z_input_7z_path),
            self.filter_args,
            schema,
            self.filter_7z_output_file_path,
            output_driver,
            property_renames=property_renames,
        )
        self.filter_worker.progress.connect(self._update_filter_7z_progress)
        self.filter_worker.finished.connect(self._handle_filter_7z_finished)
        self.filter_worker.error.connect(self._handle_filter_7z_error)
        self.filter_worker.start()

    def _update_filter_7z_schema_inference_progress(self, value):
        self.filter_7z_schema_inference_progress_bar.setValue(value)

    def _handle_filter_7z_schema_inference_error(self, error_message):
        self.filter_7z_schema_inference_label.hide()
        self.filter_7z_schema_inference_progress_bar.hide()
        self._log_status(
            f"An error occurred during schema inference: {error_message}",
            level="error",
            target_text_edit=self.filter_7z_status_text_edit,
        )

    def _handle_filter_7z_schema_inference_finished(self, result):
        self.filter_7z_schema_inference_label.hide()
        self.filter_7z_schema_inference_progress_bar.hide()
        schema, property_renames = result
        if schema is None:
            self._log_status(
                "Error: Could not infer schema from the archive.",
                level="error",
                target_text_edit=self.filter_7z_status_text_edit,
            )
            return
        self._start_filter_process(schema, property_renames)

    def _update_filter_7z_progress(self, value):
        self.filter_7z_filter_progress_bar.setValue(value)

    def _handle_filter_7z_finished(self, success):
        self.filter_7z_filter_label.hide()
        self.filter_7z_filter_progress_bar.hide()
        if success:
            self._log_status(
                "Filtering complete! You can download the output file below.",
                target_text_edit=self.filter_7z_status_text_edit,
            )
            self.filter_7z_download_button.setEnabled(True)
        else:
            self._log_status(
                "Filtering failed.",
                level="error",
                target_text_edit=self.filter_7z_status_text_edit,
            )

    def _handle_filter_7z_error(self, error_message):
        self.filter_7z_filter_label.hide()
        self.filter_7z_filter_progress_bar.hide()
        self._log_status(
            f"An error occurred during filtering: {error_message}",
            level="error",
            target_text_edit=self.filter_7z_status_text_edit,
        )

    def clear_temp_files_7z(self):
        if self.temp_dir_filter_7z_obj:
            self.temp_dir_filter_7z_obj.cleanup()
            self.temp_dir_filter_7z_obj = None

    def clear_temp_files_infer_schema(self):
        if self.temp_dir_infer_schema_obj:
            self.temp_dir_infer_schema_obj.cleanup()
            self.temp_dir_infer_schema_obj = None

    def clear_temp_files(self):
        self.clear_temp_files_7z()
        self.clear_temp_files_infer_schema()
        self._clear_temp_files_extract()

    def _download_file(self):
        if self.filter_7z_output_file_path and self.filter_7z_output_file_path.exists():
            file_dialog = QFileDialog()
            # Use the filename from the stored path (captured at run time)
            save_path, _ = file_dialog.getSaveFileName(
                self,
                "Save Filtered File",
                self.filter_7z_output_file_path.name,
                "All Files (*)",
            )
            if save_path:
                try:
                    # Handle shapefile with multiple components
                    if self.filter_7z_output_file_path.suffix.lower() == ".shp":
                        moved_files = self._move_shapefile_components(self.filter_7z_output_file_path, Path(save_path))
                        self._log_status(
                            f"Shapefile saved to: {save_path} ({len(moved_files)} files)",
                            target_text_edit=self.filter_7z_status_text_edit,
                        )
                    else:
                        Path(self.filter_7z_output_file_path).rename(save_path)
                        self._log_status(
                            f"File saved to: {save_path}",
                            target_text_edit=self.filter_7z_status_text_edit,
                        )
                    self.filter_7z_output_file_path = None  # Clear path after moving
                    self.filter_7z_download_button.setEnabled(False)  # Disable button after download
                except Exception as e:
                    self._log_status(
                        f"Error saving file: {e}",
                        level="error",
                        target_text_edit=self.filter_7z_status_text_edit,
                    )
        else:
            self._log_status(
                "No filtered file to download.",
                level="warning",
                target_text_edit=self.filter_7z_status_text_edit,
            )

        self.clear_temp_files_7z()

    def _upload_infer_schema_7z_archive(self):
        file_dialog = QFileDialog()
        file_path, _ = file_dialog.getOpenFileName(
            self, "Upload 7z Archive for Schema Inference", "", "7z Archives (*.7z)"
        )
        if file_path:
            self.infer_schema_input_7z_path = Path(file_path)
            self.infer_schema_input_7z_label.setText(f"Selected: {self.infer_schema_input_7z_path.name}")
            base_name = self.infer_schema_input_7z_path.stem
            self.infer_schema_output_file_input.setText(f"{base_name}.schema.json")
        else:
            self.infer_schema_input_7z_path = None
            self.infer_schema_input_7z_label.setText("No 7z archive selected.")
            self.infer_schema_output_file_input.setText("")

    def _run_infer_schema(self):
        self.infer_schema_output_text_edit.clear()
        self.infer_schema_download_button.setEnabled(False)
        self.infer_schema_progress_bar.setValue(0)
        self.infer_schema_progress_bar.show()

        if not self.infer_schema_input_7z_path:
            self._log_status(
                "Error: Please upload an input 7z archive.",
                level="error",
                target_text_edit=self.infer_schema_output_text_edit,
            )
            self.infer_schema_progress_bar.hide()
            return

        log_level = self.infer_schema_log_level_combo.currentText()
        logging.basicConfig(
            level=getattr(logging, log_level.upper()),
            format="%(asctime)s - %(levelname)s - %(message)s",
        )
        self._log_status(
            f"Inferring schema from {self.infer_schema_input_7z_path.name}",
            target_text_edit=self.infer_schema_output_text_edit,
        )

        self.temp_dir_infer_schema_obj = tempfile.TemporaryDirectory(delete=False, ignore_cleanup_errors=True)

        self.worker = SchemaInferenceWorker(str(self.infer_schema_input_7z_path))
        self.worker.progress.connect(self._update_infer_schema_progress)
        self.worker.finished.connect(self._handle_infer_schema_finished)
        self.worker.error.connect(self._handle_infer_schema_error)
        self.worker.start()

    def _update_infer_schema_progress(self, value):
        self.infer_schema_progress_bar.setValue(value)

    def _handle_infer_schema_finished(self, schema):
        self.infer_schema_progress_bar.hide()
        if schema is None:
            self._log_status(
                "Error: Failed to infer schema.",
                level="error",
                target_text_edit=self.infer_schema_output_text_edit,
            )
            return

        self.infer_schema_output_text_edit.setText(json.dumps(schema, indent=4))
        self._log_status(
            "Schema inferred successfully.",
            target_text_edit=self.infer_schema_output_text_edit,
        )

        # Save to a temporary file for download
        output_filename = self.infer_schema_output_file_input.text()
        if not output_filename:
            output_filename = f"{self.infer_schema_input_7z_path.stem}.schema.json"

        temp_dir_infer_schema = Path(self.temp_dir_infer_schema_obj.name)
        self.infer_schema_output_file_path = temp_dir_infer_schema / output_filename
        with open(self.infer_schema_output_file_path, "w") as f:
            json.dump(schema, f, indent=4)

        self.infer_schema_download_button.setEnabled(True)
        self._log_status(
            f"Schema saved temporarily to {self.infer_schema_output_file_path.name} for download.",
            target_text_edit=self.infer_schema_output_text_edit,
        )

    def _handle_infer_schema_error(self, error_message):
        self.infer_schema_progress_bar.hide()
        self._log_status(
            f"An error occurred during schema inference: {error_message}",
            level="error",
            target_text_edit=self.infer_schema_output_text_edit,
        )

    def _download_inferred_schema(self):
        if self.infer_schema_output_file_path and self.infer_schema_output_file_path.exists():
            file_dialog = QFileDialog()
            # Use the filename from the stored path (captured at run time)
            save_path, _ = file_dialog.getSaveFileName(
                self, "Save Inferred Schema", self.infer_schema_output_file_path.name, "JSON Files (*.json)"
            )
            if save_path:
                try:
                    Path(self.infer_schema_output_file_path).rename(save_path)
                    self._log_status(
                        f"Schema saved to: {save_path}",
                        target_text_edit=self.infer_schema_output_text_edit,
                    )
                    self.infer_schema_output_file_path = None  # Clear path after moving
                    self.infer_schema_download_button.setEnabled(False)  # Disable button after download
                except Exception as e:
                    self._log_status(
                        f"Error saving schema file: {e}",
                        level="error",
                        target_text_edit=self.infer_schema_output_text_edit,
                    )
        else:
            self._log_status(
                "No inferred schema to download.",
                level="warning",
                target_text_edit=self.infer_schema_output_text_edit,
            )

        self.clear_temp_files_infer_schema()

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
            target_text_edit = self.filter_7z_status_text_edit  # Default to filter 7z status

        if level == "error":
            target_text_edit.append(f"<p style='color:red;'>{message}</p>")
        elif level == "warning":
            target_text_edit.append(f"<p style='color:orange;'>{message}</p>")
        else:
            target_text_edit.append(f"<p style='color:blue;'>{message}</p>")
        QApplication.processEvents()  # Update UI immediately

    def closeEvent(self, event):
        self.clear_temp_files()
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
