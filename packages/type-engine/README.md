# ztap type-engine (custom component #4)

Pure-Python, infrastructure-free engine for **Postgres ↔ Delta type mapping** and
**conflict resolution**. This is the component most likely to cause *silent data
corruption* if skipped, so it is built and tested first.

## Design principle: be honest about lossiness

Every Postgres→Delta mapping carries an explicit `lossy` flag, a canonical
`encoding`, and a `note`. Types with no native Delta equivalent (`jsonb`,
`interval`, `uuid`, `cidr`, arrays, oversized `numeric`) are **not silently
coerced** — they are flagged, and round-tripping returns the *documented
substitute* rather than pretending the conversion was identity.

```python
from ztap_typeengine import map_pg_to_delta, roundtrip_pg

map_pg_to_delta("jsonb")       # LogicalType(delta_type='STRING', lossy=True, ...)
roundtrip_pg("bigint")         # ('bigint', False)
roundtrip_pg("interval")       # ('interval', True)  <- lossy flag, not identity
```

## Conflict resolution

Defined, deterministic policies for when the same primary key is updated on both
sides before a sync cycle: `LAST_WRITE_WINS`, `SOURCE_OF_TRUTH`, `MERGE`, `ERROR`.

## Test

```bash
pip install -e ".[test]"
pytest
```
