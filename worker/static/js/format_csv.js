// CSV format handler for partial downloads

export function buildCopyQuery(urlList, bboxWkt, opfsPath) {
  return `
    COPY (
      SELECT * EXCLUDE (geometry), ST_AsText(geometry) as geometry_wkt
      FROM read_parquet([${urlList}], union_by_name=true)
      WHERE ST_Intersects(geometry, ST_GeomFromText('${bboxWkt}'))
    ) TO '${opfsPath}' (FORMAT CSV, HEADER true)
  `;
}
