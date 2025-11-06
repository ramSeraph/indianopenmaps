import streamlit as st
import logging
import json
from pathlib import Path

from iomaps.commands.filter_7z import (
    create_filter,
    create_writer,
    process_archive,
    get_supported_output_drivers,
)
from iomaps.commands.schema_common import get_schema_from_archive
import py7zr
import multivolumefile
from fiona.crs import CRS
from iomaps.helpers import get_supported_input_drivers

def run_streamlit_app():
    st.set_page_config(page_title="Indianopenmaps - Filter 7z", layout="wide")
    st.title("Filter 7z Archive")

    st.sidebar.header("Input/Output")
    input_7z = st.sidebar.file_uploader("Upload 7z Archive", type=["7z"])
    import tempfile
    # Use a temporary file for output on the server
    # The user will download this file
    output_file_path = Path(tempfile.NamedTemporaryFile(delete=False, suffix=".geojsonl").name)
    # Dynamically suggest output filename based on input
    if input_7z:
        input_7z_path = Path(input_7z.name)
        base_name = input_7z_path.stem  # Gets 'a.geojsonl' from 'a.geojsonl.7z'
        suggested_output_filename = f"{base_name}.filtered.gpkg"
    else:
        suggested_output_filename = "filtered_output.gpkg"
    output_filename_for_download = st.sidebar.text_input("Suggested Output Filename", suggested_output_filename)

    st.sidebar.header("Filtering Options")
    filter_method = st.sidebar.radio("Select Filtering Method", ("Filter by File", "Filter by Bounds"))

    filter_file = None
    bounds_str = None
    filter_file_driver = None

    if filter_method == "Filter by File":
        filter_file = st.sidebar.file_uploader("Upload Filter File (e.g., Shapefile)", type=["shp", "geojson", "json"])
        filter_file_driver = st.sidebar.selectbox(
            "Filter File Driver",
            ["Infer from extension"] + get_supported_input_drivers(),
            index=0
        )
    else:
        bounds_str = st.sidebar.text_input("Bounds (min_lon,min_lat,max_lon,max_lat)", "")
    no_clip = st.sidebar.checkbox("Do not clip features by filter shape/bounds", value=False)

    st.sidebar.header("Schema and Geometry")
    schema_file = st.sidebar.file_uploader("Upload Schema File (JSON)", type=["json"])
    limit_to_geom_type = st.sidebar.selectbox(
        "Limit to Geometry Type",
        ["None", "Point", "LineString", "Polygon", "MultiPoint", "MultiLineString", "MultiPolygon", "GeometryCollection"],
        index=0
    )
    strict_geom_type_check = st.sidebar.checkbox("Strict Geometry Type Check", value=False)

    st.sidebar.header("Advanced Options")
    output_driver = st.sidebar.selectbox(
        "Output Driver",
        ["Infer from extension"] + get_supported_output_drivers(),
        index=0
    )
    log_level = st.sidebar.selectbox(
        "Log Level",
        ["INFO", "DEBUG", "WARNING", "ERROR", "CRITICAL"],
        index=0
    )

    if st.button("Run Filter"):
        if not input_7z:
            st.error("Please upload an input 7z archive.")
        elif not (filter_file or bounds_str):
            st.error("Please provide either a filter file or bounds.")
        else:
            # Setup logging
            logging.basicConfig(level=getattr(logging, log_level.upper()),
                                format='%(asctime)s - %(levelname)s - %(message)s')
            st.info(f"Processing {input_7z.name}")

            # Handle uploaded files
            input_7z_path = Path(input_7z.name)
            with open(input_7z_path, "wb") as f:
                f.write(input_7z.getbuffer())

            filter_file_path = None
            if filter_file:
                filter_file_path = Path(filter_file.name)
                with open(filter_file_path, "wb") as f:
                    f.write(filter_file.getbuffer())

            schema_path = None
            if schema_file:
                schema_path = Path(schema_file.name)
                with open(schema_path, "wb") as f:
                    f.write(schema_file.getbuffer())

            # Prepare arguments for create_filter
            filter_args = {
                "filter_file": str(filter_file_path) if filter_file_path else None,
                "filter_file_driver": filter_file_driver if filter_file_driver != "Infer from extension" else None,
                "bounds": bounds_str if bounds_str else None,
                "no_clip": no_clip,
                "limit_to_geom_type": limit_to_geom_type if limit_to_geom_type != "None" else None,
                "strict_geom_type_check": strict_geom_type_check,
            }

            filter_obj = create_filter(**filter_args)
            if filter_obj is None:
                st.error("Failed to create filter.")
                return

            # Prepare schema
            schema = None
            if schema_path:
                st.info(f"Reading schema from {schema_path}")
                with schema_path.open('r') as f:
                    schema = json.load(f)
            else:
                st.info(f"Inferring schema from {input_7z_path}")
                schema = get_schema_from_archive(str(input_7z_path), limit_to_geom_type=filter_args["limit_to_geom_type"], strict_geom_type_check=filter_args["strict_geom_type_check"])
                if schema is None:
                    st.error("Could not infer schema from the archive.")
                    return
            
            crs = CRS.from_epsg(4326)
            writer = create_writer(str(output_file_path), schema, crs, output_driver if output_driver != "Infer from extension" else None)
            if writer is None:
                st.error("Failed to create writer.")
                return

            try:
                # Need to handle multivolume files
                if str(input_7z_path).endswith('.001'):
                    base_path = str(input_7z_path).rsplit('.', 1)[0]
                    with multivolumefile.open(base_path, 'rb') as multivolume_file:
                        with py7zr.SevenZipFile(multivolume_file, 'r') as archive:
                            process_archive(archive, filter_obj, writer)
                else:
                    with py7zr.SevenZipFile(str(input_7z_path), mode='r') as archive:
                        process_archive(archive, filter_obj, writer)
                st.success("Filtering complete! You can download the output file below.")
                with open(output_file_path, "rb") as f:
                    st.download_button(
                        label="Download Filtered File",
                        data=f.read(),
                        file_name=output_filename_for_download,
                        mime="application/octet-stream"
                    )

            except Exception as e:
                st.error(f"An error occurred: {e}")
            finally:
                # Clean up the temporary output file
                if output_file_path.exists():
                    output_file_path.unlink(missing_ok=True)

if __name__ == "__main__":
    run_streamlit_app()
