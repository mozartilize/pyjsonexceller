# pyobjexceller

JSON to JSON (transformer) with Excel-like expression

## Example

Define a json schema:

```json
{
    "type": "expr",
    "mapping": ["$0.rec"],
    "ctx": {
        "rec": {
            "foo": {"foo": 1},
        }
    },
}
```

```python
schema = json.load(...)

# use particular transformer
from pyjsonexceller.transform import ExprTransformer

t = ExprTransformer(schema)
assert t() == {
    "foo": {"foo": 1},
}

# or use transformer_factory
from pyjsonexceller.transform import transformer_factory

t = transformer_factory(schema)
assert t() == {
    "foo": {"foo": 1},
}
```

Check `tests` for more usage.