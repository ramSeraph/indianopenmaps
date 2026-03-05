// GeoJSON / GeoJSONSeq format handler for partial downloads

export async function buildCopyQuery(conn, urlList, bboxWkt, opfsPath) {
  // Discover non-geometry columns
  const schemaResult = await conn.query(
    `SELECT column_name FROM (DESCRIBE SELECT * FROM read_parquet([${urlList}], union_by_name=true)) WHERE column_name != 'geometry'`
  );
  const propCols = [];
  for (let i = 0; i < schemaResult.numRows; i++) {
    propCols.push(schemaResult.getChildAt(0).get(i));
  }
  const structEntries = propCols.map(c => `'${c}', "${c}"`).join(', ');

  // Use ROW_NUMBER to prepend comma to all rows except the first
  const featureQuery = `
    SELECT CASE WHEN ROW_NUMBER() OVER () > 1 THEN ',' ELSE '' END || json_object(
      'type', 'Feature',
      'geometry', ST_AsGeoJSON(geometry)::JSON,
      'properties', json_object(${structEntries})
    ) as feature
    FROM read_parquet([${urlList}], union_by_name=true)
    WHERE ST_Intersects(geometry, ST_GeomFromText('${bboxWkt}'))
  `;

  return `
    COPY (${featureQuery}) TO '${opfsPath}' (FORMAT CSV, HEADER false, QUOTE '', DELIMITER E'\\x01')
  `;
}
