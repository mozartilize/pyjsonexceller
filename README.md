# pyobjexceller

JSON to JSON (transformer) with Excel inprised expression

## Example

Define a json schema:

```json
{
    "type": "expr",
    // $0 is the ctx (context) register
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

## Schema definition

```python
SchemaTransformerType = t.TypedDict(
    "SchemaTransformerType",
    {
        "type": str,
        "mapping": t.Any,
        "ctx": te.NotRequired[dict[str, t.Any]],
        "plugins": te.NotRequired[dict[str, t.Any]],
        "if": te.NotRequired[ExprType],
    },
)


class LiteralTransformer(Transformer):
    _mapping: t.Any


ExprType = list[t.Union[t.Any, "ExprType"]]
class ExprTransformer(Transformer):
    _mapping: ExprType


class TupleTransformer(Transformer):
    _mapping: list[SchemaTransformerType]


ListTransformerMappingType = t.TypedDict(
    "ListTransformerMappingType",
    {"iter": ExprType, "each": SchemaTransformerType},
)
class ListTransformer(Transformer):
    _mapping: ListTransformerMappingType


class ObjectTransformer(Transformer):
    _mapping: dict[str, SchemaTransformerType]
```

## Registers

You can access external data via registers.

There're currently 2 registers:

- Context `$0`

- Plugins `$1`