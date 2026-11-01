"""Regenerate the small multi-row-group fixture with the checkout's Python venv."""

import random
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

root = Path(__file__).resolve().parents[1] / "fixtures"
base = pq.read_table(root / "stac-modern.parquet")
template = base.to_pylist()[0]
rng = random.Random(7)
rows = [
    {**template, "id": f"sheet-{i:03}", "payload": rng.randbytes(512).hex()}
    for i in range(192)
]
table = pa.Table.from_pylist(rows).replace_schema_metadata(base.schema.metadata)
pq.write_table(
    table,
    root / "stac-ranges.parquet",
    row_group_size=32,
    compression="snappy",
    write_page_index=True,
)
