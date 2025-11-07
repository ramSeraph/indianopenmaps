import sys
import logging
import json
from pathlib import Path
import tempfile

from PyQt5.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
    QLabel, QPushButton, QFileDialog, QRadioButton, QComboBox,
    QCheckBox, QLineEdit, QTextEdit, QButtonGroup, QGroupBox, QStackedWidget,
    QScrollArea
)

import fiona
import py7zr
import multivolumefile
from fiona.crs import CRS
from fiona.model import to_dict

from iomaps.commands.filter_7z import (
    create_filter,
    create_writer,
    process_archive,
)
from iomaps.commands.schema_common import get_schema_from_archive
from iomaps.helpers import get_supported_input_drivers

class PyQtApp(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Indianopenmaps - Filter 7z")
        self.setGeometry(100, 100, 800, 600)

        self.setStyleSheet("QLabel { qproperty-alignment: AlignLeft; }")

        self.central_widget = QWidget()
        self.setCentralWidget(self.central_widget)
        self.main_layout = QHBoxLayout(self.central_widget)

        self.sidebar_layout = QVBoxLayout()
        self.content_stack = QStackedWidget() # Use QStackedWidget to switch between views

        self.main_layout.addLayout(self.sidebar_layout, 1)
        self.main_layout.addWidget(self.content_stack, 3)

        self.temp_dir_filter_7z_obj = None
        self.temp_dir_infer_schema_obj = None

        # Internal state variables for Filter 7z
        self.filter_7z_input_7z_path = None
        self.filter_7z_filter_file_path = None
        self.filter_7z_schema_file_path = None
        self.filter_7z_output_file_path = None
        self.filter_7z_suggested_output_filename = "filtered_output.gpkg"
        self.filter_7z_features = []

        # Internal state variables for Infer Schema
        self.infer_schema_input_7z_path = None
        self.infer_schema_output_file_path = None # Path where the inferred schema will be saved temporarily

        self._setup_ui()

    def _setup_ui(self):
        # Sidebar for tool selection
        self.filter_7z_button = QPushButton("Filter 7z")
        self.filter_7z_button.clicked.connect(lambda: self._show_tool_view("filter_7z"))
        self.infer_schema_button = QPushButton("Infer Schema")
        self.infer_schema_button.clicked.connect(lambda: self._show_tool_view("infer_schema"))

        self.sidebar_layout.addWidget(self.filter_7z_button)
        self.sidebar_layout.addWidget(self.infer_schema_button)
        self.sidebar_layout.addStretch() # Push buttons to the top

        # --- Filter 7z Widget ---
        self.filter_7z_widget = QWidget()
        filter_7z_layout = QVBoxLayout(self.filter_7z_widget)

        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        scroll_content = QWidget()
        scroll.setWidget(scroll_content)
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
        self.filter_7z_output_type_combo.addItems(["GPKG", "Shapefile", "GeoJSON"])
        self.filter_7z_output_type_combo.currentTextChanged.connect(self._update_suggested_output_filename)
        filter_7z_input_output_layout.addWidget(QLabel("Output Type:"))
        filter_7z_input_output_layout.addWidget(self.filter_7z_output_type_combo)

        self.filter_7z_output_filename_input = QLineEdit("filtered_output.gpkg")
        filter_7z_input_output_layout.addWidget(QLabel("Suggested Output Filename:"))
        filter_7z_input_output_layout.addWidget(self.filter_7z_output_filename_input)

        filter_7z_input_output_group.setLayout(filter_7z_input_output_layout)
        filter_7z_scroll_layout.addWidget(filter_7z_input_output_group)

        # Filtering Options Section for Filter 7z
        filter_7z_filtering_options_group = QGroupBox("Filtering Options")
        filter_7z_filtering_options_layout = QVBoxLayout()

        self.filter_7z_filter_type_group = QButtonGroup(self)
        self.filter_7z_filter_file_radio = QRadioButton("Filter File")
        self.filter_7z_bounds_radio = QRadioButton("Bounds")
        self.filter_7z_filter_type_group.addButton(self.filter_7z_filter_file_radio)
        self.filter_7z_filter_type_group.addButton(self.filter_7z_bounds_radio)
        self.filter_7z_filter_file_radio.setChecked(True) # Default selection

        filter_7z_filtering_options_layout.addWidget(self.filter_7z_filter_file_radio)
        filter_7z_filtering_options_layout.addWidget(self.filter_7z_bounds_radio)

        self.filter_7z_filter_file_widgets = QWidget()
        filter_7z_filter_file_layout = QVBoxLayout(self.filter_7z_filter_file_widgets)
        self.filter_7z_upload_filter_file_button = QPushButton("Upload Filter File (e.g., Shapefile, GeoJSON)")
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
        filter_7z_bounds_layout.addWidget(self.filter_7z_bounds_input)
        filter_7z_filtering_options_layout.addWidget(self.filter_7z_bounds_widgets)
        self.filter_7z_bounds_widgets.hide() # Hide by default

        self.filter_7z_filter_file_radio.toggled.connect(self._toggle_filter_type_widgets)
        self.filter_7z_bounds_radio.toggled.connect(self._toggle_filter_type_widgets)

        self.filter_7z_no_clip_checkbox = QCheckBox("Do not clip features by filter shape/bounds")
        filter_7z_filtering_options_layout.addWidget(self.filter_7z_no_clip_checkbox)

        filter_7z_filtering_options_group.setLayout(filter_7z_filtering_options_layout)
        filter_7z_scroll_layout.addWidget(filter_7z_filtering_options_group)

        # Schema and Geometry Section for Filter 7z
        filter_7z_schema_geometry_group = QGroupBox("Schema and Geometry")
        filter_7z_schema_geometry_layout = QVBoxLayout()

        self.filter_7z_upload_schema_button = QPushButton("Upload Schema File (JSON)")
        self.filter_7z_upload_schema_button.clicked.connect(self._upload_schema_file)
        self.filter_7z_upload_schema_button.setToolTip("Upload a JSON schema file to define the expected structure and types of features in the output. If not provided, an extra pass is done over the data to infer the schema automatically.")
        self.filter_7z_schema_file_label = QLabel("No schema file selected.")
        filter_7z_schema_geometry_layout.addWidget(self.filter_7z_upload_schema_button)
        filter_7z_schema_geometry_layout.addWidget(self.filter_7z_schema_file_label)

        self.filter_7z_limit_geom_type_combo = QComboBox()
        self.filter_7z_limit_geom_type_combo.addItems(["None", "Point", "LineString", "Polygon", "MultiPoint", "MultiLineString", "MultiPolygon", "GeometryCollection"])
        filter_7z_schema_geometry_layout.addWidget(QLabel("Limit to Geometry Type:"))
        filter_7z_schema_geometry_layout.addWidget(self.filter_7z_limit_geom_type_combo)

        self.filter_7z_strict_geom_type_checkbox = QCheckBox("Strict Geometry Type Check")
        filter_7z_schema_geometry_layout.addWidget(self.filter_7z_strict_geom_type_checkbox)

        filter_7z_schema_geometry_group.setLayout(filter_7z_schema_geometry_layout)

        # Advanced Options Section for Filter 7z
        filter_7z_advanced_options_group = QGroupBox("Advanced Options")
        filter_7z_advanced_options_group.setCheckable(True)
        filter_7z_advanced_options_group.setChecked(False)
        filter_7z_advanced_options_layout = QVBoxLayout()

        filter_7z_advanced_options_layout.addWidget(filter_7z_schema_geometry_group)

        self.filter_7z_log_level_combo = QComboBox()
        self.filter_7z_log_level_combo.addItems(["INFO", "DEBUG", "WARNING", "ERROR", "CRITICAL"])
        filter_7z_advanced_options_layout.addWidget(QLabel("Log Level:"))
        filter_7z_advanced_options_layout.addWidget(self.filter_7z_log_level_combo)

        filter_7z_advanced_options_group.setLayout(filter_7z_advanced_options_layout)
        filter_7z_scroll_layout.addWidget(filter_7z_advanced_options_group)

        filter_7z_layout.addWidget(scroll)

        # Main Content - Run Filter Button and Output for Filter 7z
        self.filter_7z_run_filter_button = QPushButton("Run Filter")
        self.filter_7z_run_filter_button.clicked.connect(self._run_filter)
        filter_7z_layout.addWidget(self.filter_7z_run_filter_button)

        self.filter_7z_status_text_edit = QTextEdit()
        self.filter_7z_status_text_edit.setReadOnly(True)
        filter_7z_layout.addWidget(self.filter_7z_status_text_edit)

        self.filter_7z_download_button = QPushButton("Download Filtered File")
        self.filter_7z_download_button.clicked.connect(self._download_file)
        self.filter_7z_download_button.setEnabled(False) # Disabled until filtering is complete
        filter_7z_layout.addWidget(self.filter_7z_download_button)

        self.content_stack.addWidget(self.filter_7z_widget) # Add filter_7z_widget to the stack

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

        self.infer_schema_limit_geom_type_combo = QComboBox()
        self.infer_schema_limit_geom_type_combo.addItems(["None", "Point", "LineString", "Polygon", "MultiPoint", "MultiLineString", "MultiPolygon", "GeometryCollection"])
        infer_schema_options_layout.addWidget(QLabel("Limit to Geometry Type:"))
        infer_schema_options_layout.addWidget(self.infer_schema_limit_geom_type_combo)

        self.infer_schema_strict_geom_type_checkbox = QCheckBox("Strict Geometry Type Check")
        infer_schema_options_layout.addWidget(self.infer_schema_strict_geom_type_checkbox)

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

        self.infer_schema_output_text_edit = QTextEdit()
        self.infer_schema_output_text_edit.setReadOnly(True)
        self.infer_schema_output_text_edit.setMaximumHeight(150) # Set a maximum height
        infer_schema_layout.addWidget(self.infer_schema_output_text_edit)

        # Download Options for Infer Schema
        infer_schema_download_group = QGroupBox("Download Options")
        infer_schema_download_layout = QVBoxLayout()

        self.infer_schema_output_file_input = QLineEdit()
        self.infer_schema_output_file_input.setPlaceholderText("Optional: Path for output schema file (e.g., schema.json)")
        infer_schema_download_layout.addWidget(QLabel("Output Schema File:"))
        infer_schema_download_layout.addWidget(self.infer_schema_output_file_input)

        self.infer_schema_download_button = QPushButton("Download Schema File")
        self.infer_schema_download_button.clicked.connect(self._download_inferred_schema)
        self.infer_schema_download_button.setEnabled(False)
        infer_schema_download_layout.addWidget(self.infer_schema_download_button)

        infer_schema_download_group.setLayout(infer_schema_download_layout)
        infer_schema_layout.addWidget(infer_schema_download_group)
        infer_schema_layout.addStretch() # Add stretch to prevent download group from stretching

        self.content_stack.addWidget(self.infer_schema_widget) # Add infer_schema_widget to the stack

        # Set initial view
        self._show_tool_view("filter_7z")

    def _show_tool_view(self, tool_name):
        if tool_name == "filter_7z":
            self.content_stack.setCurrentWidget(self.filter_7z_widget)
            self.setWindowTitle("Indianopenmaps - Filter 7z")
        elif tool_name == "infer_schema":
            self.content_stack.setCurrentWidget(self.infer_schema_widget)
            self.setWindowTitle("Indianopenmaps - Infer Schema")

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
        file_path, _ = file_dialog.getOpenFileName(self, "Upload Filter File", "", "Filter Files (*.shp *.geojson *.json *.geojsonl)")
        self.filter_7z_feature_combo.clear()
        self.filter_7z_features = []
        self.filter_7z_feature_label.hide()
        self.filter_7z_feature_combo.hide()
        if file_path:
            self.filter_7z_filter_file_path = Path(file_path)
            self.filter_7z_filter_file_label.setText(f"Selected: {self.filter_7z_filter_file_path.name}")
            count = 0
            try:
                with fiona.open(file_path, 'r') as collection:
                    for feature in collection:
                        # if gemotry type is not Polygon, MultiPolygon, skip
                        if feature['geometry']['type'] not in ['Polygon', 'MultiPolygon']:
                            continue
                        self.filter_7z_features.append(feature)
                        # Create a display text for the dropdown
                        properties = None
                        if 'properties' in feature:
                            properties = ', '.join([f"{k}: {v}" for k, v in feature['properties'].items()])
                        display_text = f"Feature {count} - {properties}"
                        self.filter_7z_feature_combo.addItem(display_text)
                        count += 1
                if len(self.filter_7z_features) > 1:
                    self.filter_7z_feature_label.show()
                    self.filter_7z_feature_combo.show()
            except Exception as e:
                self._log_status(f"Error reading filter file: {e}", level="error", target_text_edit=self.filter_7z_status_text_edit)
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
            self._log_status("Error: Please upload an input 7z archive.", level="error", target_text_edit=self.filter_7z_status_text_edit)
            return

        filter_type = "Filter File" if self.filter_7z_filter_file_radio.isChecked() else "Bounds"
        if filter_type == "Filter File" and not self.filter_7z_filter_file_path:
            self._log_status("Error: Please upload a valid filter file.", level="error", target_text_edit=self.filter_7z_status_text_edit)
            return
        elif filter_type == "Bounds" and not self.filter_7z_bounds_input.text():
            self._log_status("Error: Please enter bounds.", level="error", target_text_edit=self.filter_7z_status_text_edit)
            return

        # Setup logging
        log_level = self.filter_7z_log_level_combo.currentText()
        logging.basicConfig(level=getattr(logging, log_level.upper()),
                            format='%(asctime)s - %(levelname)s - %(message)s')
        self._log_status(f"Processing {self.filter_7z_input_7z_path.name}", target_text_edit=self.filter_7z_status_text_edit)

        schema_path_for_processing = None
        if self.filter_7z_schema_file_path:
            schema_path_for_processing = self.filter_7z_schema_file_path

        # run this in the context of a temporary directory
        self.temp_dir_filter_7z_obj = tempfile.TemporaryDirectory(delete=False, ignore_cleanup_errors=True)
        temp_dir_filter_7z = Path(self.temp_dir_filter_7z_obj.name)

        filter_file_for_processing = self.filter_7z_filter_file_path
        if len(self.filter_7z_features) > 1:
            selected_index = self.filter_7z_feature_combo.currentIndex()
            selected_feature = self.filter_7z_features[selected_index]

            # Write the selected feature to a temporary file
            temp_filter_file = temp_dir_filter_7z / "temp_filter.geojson"

            # Create a GeoJSON FeatureCollection with a single feature
            feature_collection = {
                "type": "FeatureCollection",
                "features": [to_dict(selected_feature)]
            }
            temp_filter_file.write_text(json.dumps(feature_collection))
            filter_file_for_processing = temp_filter_file
        
        # Prepare arguments for create_filter
        filter_args = {
            "filter_file": str(filter_file_for_processing) if filter_file_for_processing and filter_type == "Filter File" else None,
            "filter_file_driver": self.filter_7z_filter_file_driver_combo.currentText() if self.filter_7z_filter_file_driver_combo.currentText() != "Infer from extension" else None,
            "bounds": self.filter_7z_bounds_input.text() if filter_type == "Bounds" else None,
            "no_clip": self.filter_7z_no_clip_checkbox.isChecked(),
            "limit_to_geom_type": self.filter_7z_limit_geom_type_combo.currentText() if self.filter_7z_limit_geom_type_combo.currentText() != "None" else None,
            "strict_geom_type_check": self.filter_7z_strict_geom_type_checkbox.isChecked(),
        }

        filter_obj = create_filter(**filter_args)
        if filter_obj is None:
            self._log_status("Error: Failed to create filter.", level="error", target_text_edit=self.filter_7z_status_text_edit)
            return

        # Prepare schema
        schema = None
        if schema_path_for_processing:
            self._log_status(f"Reading schema from {schema_path_for_processing}", target_text_edit=self.filter_7z_status_text_edit)
            with schema_path_for_processing.open('r') as f:
                schema = json.load(f)
        else:
            self._log_status(f"Inferring schema from {self.filter_7z_input_7z_path}", target_text_edit=self.filter_7z_status_text_edit)
            schema = get_schema_from_archive(str(self.filter_7z_input_7z_path), limit_to_geom_type=filter_args["limit_to_geom_type"], strict_geom_type_check=filter_args["strict_geom_type_check"])
            if schema is None:
                self._log_status("Error: Could not infer schema from the archive.", level="error", target_text_edit=self.filter_7z_status_text_edit)
                return
        
        self.filter_7z_output_file_path = temp_dir_filter_7z / self.filter_7z_output_filename_input.text()

        output_type = self.filter_7z_output_type_combo.currentText()
        output_driver = output_type

        crs = CRS.from_epsg(4326)
        writer = create_writer(str(self.filter_7z_output_file_path), schema, crs, output_driver if output_driver else None)
        if writer is None:
            self._log_status("Error: Failed to create writer.", level="error", target_text_edit=self.filter_7z_status_text_edit)
            return

        try:
            if str(self.filter_7z_input_7z_path).endswith('.001'):
                base_path = str(self.filter_7z_input_7z_path).rsplit('.', 1)[0]
                with multivolumefile.open(base_path, 'rb') as multivolume_file:
                    with py7zr.SevenZipFile(multivolume_file, 'r') as archive:
                        process_archive(archive, filter_obj, writer)
            else:
                with py7zr.SevenZipFile(str(self.filter_7z_input_7z_path), mode='r') as archive:
                    process_archive(archive, filter_obj, writer)
            self._log_status("Filtering complete! You can download the output file below.", target_text_edit=self.filter_7z_status_text_edit)
            self.filter_7z_download_button.setEnabled(True)

        except Exception as e:
            self._log_status(f"An error occurred: {e}", level="error", target_text_edit=self.filter_7z_status_text_edit)
        finally:
            # Clean up temporary files (except the output file for download)
            pass # Cleanup will happen on app exit or explicitly when needed

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

    def  _download_file(self):
        if self.filter_7z_output_file_path and self.filter_7z_output_file_path.exists():
            file_dialog = QFileDialog()
            save_path, _ = file_dialog.getSaveFileName(self, "Save Filtered File", self.filter_7z_output_filename_input.text(), "All Files (*)")
            if save_path:
                try:
                    Path(self.filter_7z_output_file_path).rename(save_path)
                    self._log_status(f"File saved to: {save_path}", target_text_edit=self.filter_7z_status_text_edit)
                    self.filter_7z_output_file_path = None # Clear path after moving
                    self.filter_7z_download_button.setEnabled(False) # Disable button after download
                except Exception as e:
                    self._log_status(f"Error saving file: {e}", level="error", target_text_edit=self.filter_7z_status_text_edit)
        else:
            self._log_status("No filtered file to download.", level="warning", target_text_edit=self.filter_7z_status_text_edit)

        self.clear_temp_files_7z()

    def _upload_infer_schema_7z_archive(self):
        file_dialog = QFileDialog()
        file_path, _ = file_dialog.getOpenFileName(self, "Upload 7z Archive for Schema Inference", "", "7z Archives (*.7z)")
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

        if not self.infer_schema_input_7z_path:
            self._log_status("Error: Please upload an input 7z archive.", level="error", target_text_edit=self.infer_schema_output_text_edit)
            return

        log_level = self.infer_schema_log_level_combo.currentText()
        logging.basicConfig(level=getattr(logging, log_level.upper()),
                            format='%(asctime)s - %(levelname)s - %(message)s')
        self._log_status(f"Inferring schema from {self.infer_schema_input_7z_path.name}", target_text_edit=self.infer_schema_output_text_edit)

        limit_to_geom_type = self.infer_schema_limit_geom_type_combo.currentText()
        if limit_to_geom_type == "None":
            limit_to_geom_type = None
        strict_geom_type_check = self.infer_schema_strict_geom_type_checkbox.isChecked()

        self.temp_dir_infer_schema_obj = tempfile.TemporaryDirectory(delete=False, ignore_cleanup_errors=True)
        temp_dir_infer_schema = Path(self.temp_dir_infer_schema_obj.name)

        try:
            schema = get_schema_from_archive(
                str(self.infer_schema_input_7z_path),
                limit_to_geom_type=limit_to_geom_type,
                strict_geom_type_check=strict_geom_type_check
            )

            if schema is None:
                self._log_status("Error: Failed to infer schema.", level="error", target_text_edit=self.infer_schema_output_text_edit)
                return

            self.infer_schema_output_text_edit.setText(json.dumps(schema, indent=4))
            self._log_status("Schema inferred successfully.", target_text_edit=self.infer_schema_output_text_edit)

            # Save to a temporary file for download
            output_filename = self.infer_schema_output_file_input.text()
            if not output_filename:
                output_filename = f"{self.infer_schema_input_7z_path.stem}.schema.json"
            
            self.infer_schema_output_file_path = temp_dir_infer_schema / output_filename
            with open(self.infer_schema_output_file_path, 'w') as f:
                json.dump(schema, f, indent=4)
            
            self.infer_schema_download_button.setEnabled(True)
            self._log_status(f"Schema saved temporarily to {self.infer_schema_output_file_path.name} for download.", target_text_edit=self.infer_schema_output_text_edit)

        except Exception as e:
            self._log_status(f"An error occurred during schema inference: {e}", level="error", target_text_edit=self.infer_schema_output_text_edit)

    def _download_inferred_schema(self):
        if self.infer_schema_output_file_path and self.infer_schema_output_file_path.exists():
            file_dialog = QFileDialog()
            suggested_filename = self.infer_schema_output_file_input.text()
            if not suggested_filename:
                suggested_filename = f"{self.infer_schema_input_7z_path.stem}.schema.json"

            save_path, _ = file_dialog.getSaveFileName(self, "Save Inferred Schema", suggested_filename, "JSON Files (*.json)")
            if save_path:
                try:
                    Path(self.infer_schema_output_file_path).rename(save_path)
                    self._log_status(f"Schema saved to: {save_path}", target_text_edit=self.infer_schema_output_text_edit)
                    self.infer_schema_output_file_path = None # Clear path after moving
                    self.infer_schema_download_button.setEnabled(False) # Disable button after download
                except Exception as e:
                    self._log_status(f"Error saving schema file: {e}", level="error", target_text_edit=self.infer_schema_output_text_edit)
        else:
            self._log_status("No inferred schema to download.", level="warning", target_text_edit=self.infer_schema_output_text_edit)

        self.clear_temp_files_infer_schema()

    def _log_status(self, message, level="info", target_text_edit=None):
        if target_text_edit is None:
            target_text_edit = self.filter_7z_status_text_edit # Default to filter 7z status

        if level == "error":
            target_text_edit.append(f"<p style='color:red;'>{message}</p>")
        elif level == "warning":
            target_text_edit.append(f"<p style='color:orange;'>{message}</p>")
        else:
            target_text_edit.append(f"<p style='color:blue;'>{message}</p>")
        QApplication.processEvents() # Update UI immediately

    def closeEvent(self, event):
        self.clear_temp_files()
        super().closeEvent(event)

def main():
    app = QApplication(sys.argv)
    window = PyQtApp()
    window.show()
    sys.exit(app.exec_())

if __name__ == "__main__":
    main()
