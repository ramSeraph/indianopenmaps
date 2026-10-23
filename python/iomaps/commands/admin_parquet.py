"""Convert large WGS84 GeoJSONL files without materializing an Arrow table in RAM."""

import json
import math
import tempfile
from datetime import UTC, datetime
from pathlib import Path

import click
import duckdb

from iomaps.core.geometry_validation import CoordinateError, validate_wkb_batches


def sql_string(value):
    return "'" + str(value).replace("'", "''") + "'"


def identifier(value):
    return '"' + value.replace('"', '""') + '"'


def is_geometry_type(value):
    # DuckDB 1.5 includes the CRS in the type, e.g. GEOMETRY('EPSG:4326').
    return value == "GEOMETRY" or value.startswith("GEOMETRY(")


def progress(message):
    click.echo(f"{datetime.now(UTC).isoformat(timespec='seconds')} {message}")


def geometry_metadata(conn, query, geometry):
    """Aggregate metadata in DuckDB; only the small aggregate reaches Python."""
    geom = identifier(geometry)
    row = conn.execute(
        f"SELECT count(*), min(ST_XMin({geom})), min(ST_YMin({geom})), "
        f"max(ST_XMax({geom})), max(ST_YMax({geom})), "
        f"list(DISTINCT ST_GeometryType({geom})::VARCHAR) FILTER (WHERE {geom} IS NOT NULL) "
        f"FROM ({query})"
    ).fetchone()
    count, *bounds, types = row
    if not count or any(value is None or not math.isfinite(value) for value in bounds):
        raise click.ClickException("Cannot export an empty dataset or non-finite geometry bounds")
    type_names = {
        "POINT": "Point",
        "LINESTRING": "LineString",
        "POLYGON": "Polygon",
        "MULTIPOINT": "MultiPoint",
        "MULTILINESTRING": "MultiLineString",
        "MULTIPOLYGON": "MultiPolygon",
        "GEOMETRYCOLLECTION": "GeometryCollection",
    }
    if any(value not in type_names for value in types):
        raise click.ClickException(f"Unsupported geometry types: {types}")
    # ST_GeometryType does not distinguish Z/M. Refuse to mislabel such inputs.
    if conn.execute(f"SELECT coalesce(max(ST_ZMFlag({geom})), 0) FROM ({query})").fetchone()[0]:
        raise click.ClickException("--bounded-parquet currently supports 2D geometries only")
    meta = {
        "version": "1.1.0",
        "primary_column": "geometry",
        "columns": {
            "geometry": {
                "encoding": "WKB",
                "geometry_types": sorted(type_names[t] for t in types),
                "bbox": bounds,
                "covering": {"bbox": {axis: ["bbox", axis] for axis in ("xmin", "ymin", "xmax", "ymax")}},
            }
        },
    }
    return count, meta


def copy_parquet(conn, query, path, metadata):
    """DuckDB COPY streams row groups; its sort can spill to the configured data disk."""
    conn.execute(
        f"COPY ({query}) TO {sql_string(path)} (FORMAT PARQUET, COMPRESSION ZSTD, "
        # ZSTD 22 and large row groups can allocate substantial memory outside
        # DuckDB's buffer limit. Use smaller groups and a moderate compression level.
        f"COMPRESSION_LEVEL 9, ROW_GROUP_SIZE 8192, "
        f"KV_METADATA {{geo: {sql_string(json.dumps(metadata))}}})"
    )


def source_identity(source):
    info = source.stat()
    return {"path": str(source), "bytes": info.st_size, "mtime_ns": info.st_mtime_ns}


def bounded_parquet(source, output, memory_limit="256MB", max_bytes=2 * 1024**3, resume_db=None):
    """Use an external DuckDB spool, spatial sort, and bounded COPY for every part.

    The original is kept throughout. Temporary files stay under TMPDIR. Finished
    outputs appear only after conversion, partition-size and row-count checks.
    """
    source, output = Path(source).resolve(), Path(output).resolve()
    if output.exists() or list(output.parent.glob(output.stem + ".part*.parquet")):
        raise click.ClickException("Parquet output already exists; inspect it before rerunning")
    with tempfile.TemporaryDirectory(prefix="iomaps-parquet-") as directory:
        work = Path(directory)
        database = Path(resume_db).resolve() if resume_db else work / "features.duckdb"
        if resume_db and not database.is_file():
            raise click.ClickException("Resume database does not exist")
        conn = duckdb.connect(
            str(database),
            config={
                "memory_limit": memory_limit,
                "threads": 1,
                "temp_directory": str(work / "spill"),
                "preserve_insertion_order": False,
            },
        )
        try:
            try:
                conn.execute("LOAD spatial")
            except duckdb.Error:
                conn.execute("INSTALL spatial")
                conn.execute("LOAD spatial")
            # We supply GeoParquet 1.1 metadata with bbox covering ourselves.
            # Automatic spatial conversion can otherwise replace it with 1.0.
            conn.execute("SET enable_geoparquet_conversion=false")
            progress(f"DuckDB memory limit {memory_limit}, working directory {work}")
            if resume_db:
                checkpoint = conn.execute("SELECT source_json, feature_count FROM iomaps_import").fetchone()
                if not checkpoint or json.loads(checkpoint[0]) != source_identity(source):
                    raise click.ClickException("Resume database belongs to a different or changed source file")
                if conn.execute("SELECT count(*) FROM features").fetchone()[0] != checkpoint[1]:
                    raise click.ClickException("Resume database feature count differs from its checkpoint")
                progress(f"Resuming the completed import of {checkpoint[1]:,} features from {database}")
            else:
                progress("Checking source CRS")
                layers = conn.execute("SELECT layers FROM ST_Read_Meta(?)", [str(source)]).fetchone()[0]
                if len(layers) != 1 or len(layers[0]["geometry_fields"]) != 1:
                    raise click.ClickException("Expected one source layer with one geometry field")
                crs = layers[0]["geometry_fields"][0].get("crs") or {}
                if (crs.get("auth_name"), crs.get("auth_code")) not in (("EPSG", "4326"), ("OGC", "CRS84")):
                    raise click.ClickException(f"--bounded-parquet requires WGS84 GeoJSONL; source CRS: {crs}")
                before = source_identity(source)
                progress("Importing GeoJSONL into the disk-backed table")
                conn.execute(f"CREATE TABLE features AS SELECT * FROM ST_Read({sql_string(source)})")
                if source_identity(source) != before:
                    raise click.ClickException("Source changed while importing")
                conn.execute("CREATE TABLE iomaps_import (source_json VARCHAR, feature_count UBIGINT)")
                conn.execute("INSERT INTO iomaps_import SELECT ?, count(*) FROM features", [json.dumps(before)])
                conn.execute("CHECKPOINT")
            schema = conn.execute("DESCRIBE features").fetchall()
            geometries = [row[0] for row in schema if is_geometry_type(row[1])]
            if len(geometries) != 1:
                raise click.ClickException("Expected one GEOMETRY column")
            geometry = geometries[0]
            if any(row[0] in ("geometry", "bbox", "file_row_number") and row[0] != geometry for row in schema):
                raise click.ClickException("Source properties conflict with reserved GeoParquet column names")
            progress("Validating all WGS84 coordinates in the imported table")
            try:
                batches = conn.sql(f"SELECT ST_AsWKB({identifier(geometry)}) FROM features").to_arrow_reader(1024)
                validate_wkb_batches(batches, "imported geometry", progress=progress)
            except CoordinateError as exc:
                raise click.ClickException(str(exc)) from exc
            progress("Computing geometry bounds and types from the disk-backed table")
            count, metadata = geometry_metadata(conn, "SELECT * FROM features", geometry)
            bounds = metadata["columns"]["geometry"]["bbox"]
            geom = identifier(geometry)
            envelope = "ST_Extent(ST_MakeEnvelope(" + ",".join(map(str, bounds)) + "))"
            query = (
                f"SELECT * EXCLUDE ({geom}), ST_AsWKB({geom})::BLOB AS geometry, "
                f"struct_pack(xmin := ST_XMin({geom}), ymin := ST_YMin({geom}), "
                f"xmax := ST_XMax({geom}), ymax := ST_YMax({geom})) AS bbox "
                f"FROM features ORDER BY ST_Hilbert({geom}, {envelope})"
            )
            complete = work / output.name
            progress(f"Spatially sorting and writing {count:,} features with disk spill enabled")
            copy_parquet(conn, query, complete, metadata)
            if complete.stat().st_size <= max_bytes:
                finished = [complete]
            else:
                # Contiguous Hilbert-ordered row ranges keep spatial locality. Split
                # oversized ranges recursively; never use the whole-table Arrow writer.
                finished = []
                pending = [(0, count)]
                while pending:
                    start, size = pending.pop(0)
                    selection = (
                        f"SELECT * EXCLUDE (file_row_number) FROM read_parquet({sql_string(complete)}, "
                        f"file_row_number=true) WHERE file_row_number >= {start} "
                        f"AND file_row_number < {start + size}"
                    )
                    if start == 0 and size == count:
                        if size < 2:
                            raise click.ClickException("A single feature exceeds the release asset size limit")
                        half = size // 2
                        pending[:0] = [(start, half), (start + half, size - half)]
                        continue
                    # Spatial-enabled DuckDB may already expose geometry as GEOMETRY;
                    # read_parquet otherwise returns WKB. Normalize only for metadata.
                    dtype = conn.execute(f"DESCRIBE ({selection})").fetchall()
                    geometry_type = next(row[1] for row in dtype if row[0] == "geometry")
                    typed = (
                        selection
                        if is_geometry_type(geometry_type)
                        else (f"SELECT * REPLACE (ST_GeomFromWKB(geometry) AS geometry) FROM ({selection})")
                    )
                    part_count, part_meta = geometry_metadata(conn, typed, "geometry")
                    assert part_count == size
                    part = work / f"{output.stem}.part{len(finished):04d}.parquet"
                    part_query = (
                        selection
                        if not is_geometry_type(geometry_type)
                        else (f"SELECT * REPLACE (ST_AsWKB(geometry)::BLOB AS geometry) FROM ({selection})")
                    )
                    progress(f"Writing partition rows {start:,}–{start + size:,}")
                    copy_parquet(conn, part_query, part, part_meta)
                    if part.stat().st_size > max_bytes:
                        part.unlink()
                        if size < 2:
                            raise click.ClickException("A single feature exceeds the release asset size limit")
                        half = size // 2
                        pending[:0] = [(start, half), (start + half, size - half)]
                    else:
                        finished.append(part)
            actual = conn.read_parquet([str(p) for p in finished]).count("*").fetchall()[0][0]
            if actual != count:
                raise click.ClickException(f"Parquet feature count mismatch: {actual} vs {count}")
            destinations = [output.parent / p.name for p in finished]
            if any(path.exists() for path in destinations):
                raise click.ClickException("Output appeared during conversion; refusing to overwrite it")
            for path, destination in zip(finished, destinations, strict=True):
                path.rename(destination)
            progress(f"GeoParquet complete: {count:,} features, {len(destinations)} file(s)")
            return destinations
        finally:
            conn.close()
