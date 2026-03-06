// GeoJSON / GeoJSONSeq format handler for partial downloads

export async function buildCopyQuery(conn, urlList, bboxWkt, opfsPath, { commaSeparated = false } = {}) {
  // Discover non-geometry columns
  const schemaResult = await conn.query(
    `SELECT column_name FROM (DESCRIBE SELECT * FROM read_parquet([${urlList}], union_by_name=true)) WHERE column_name != 'geometry'`
  );
  const propCols = [];
  for (let i = 0; i < schemaResult.numRows; i++) {
    propCols.push(schemaResult.getChildAt(0).get(i));
  }
  const structEntries = propCols.map(c => `'${c}', "${c}"`).join(', ');

  const jsonExpr = `json_object(
      'type', 'Feature',
      'geometry', ST_AsGeoJSON(geometry)::JSON,
      'properties', json_object(${structEntries})
    )`;

  // For GeoJSON FeatureCollection, prepend comma to all rows except the first
  const selectExpr = commaSeparated
    ? `CASE WHEN ROW_NUMBER() OVER () > 1 THEN ',' ELSE '' END || ${jsonExpr}`
    : jsonExpr;

  const featureQuery = `
    SELECT ${selectExpr} as feature
    FROM read_parquet([${urlList}], union_by_name=true)
    WHERE ST_Intersects(geometry, ST_GeomFromText('${bboxWkt}'))
  `;

  return `
    COPY (${featureQuery}) TO '${opfsPath}' (FORMAT CSV, HEADER false, QUOTE '', DELIMITER E'\\x01')
  `;
}
