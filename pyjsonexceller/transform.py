import builtins
import operator
import typing as t
from collections.abc import Iterable
from enum import Enum

import typing_extensions as te

from pyjsonexceller.exceptions import (
    FunctionNotFoundError,
    AttributeNotFound,
    PluginNotFoundError,
)

"""
["filter", ["__ge__", 10, ["getattr", "$1.my_plugin:x", "value"]]]

{
    "type": "object",
    "mapping": {
        "id": {
            "mapping": ["concat", "ORDER-", ["getitem", "$rec", "PO"]],
            "type": "expr",
        },
        "lifeCycleState": {
            "mapping": "Prebuilt",
            "type": "literal",
        },
        "lifeCycleState": {
            // TODO:
            "mapping": ["if", [], "Prebuilt"],
            "type": "expr",
        },
        "references": {
            "type": "tuple",
            "mapping": [
                {
                    "type": "object",
                    "mapping": {
                        "qualifier": {
                            "type": "literal",
                            "mapping": "foo",
                        },
                        "value": {
                            "type": "expr",
                            "mapping": ["getitem", ["getitem", "$rec", "foo"], "foo"]
                        },
                    },
                    // TODO:
                    "if": []
                }
            ]
        },
    },
    "ctx": {
        "rec": {
            "foo": {
                "foo": 1
            },
            "PO": "PO",
        }
    }
}

{
    "type": "list",
    "mapping": {
        "iter": ["getattr", "items"],
        "each": {
        }
    }
}
"""


class MappingType(str, Enum):
    EXPR = "expr"
    LITERAL = "literal"
    TUPLE = "tuple"
    LIST = "list"
    OBJECT = "object"


ExprType = list[t.Union[t.Any, "ExprType"]]


SchemaTransformerType = t.TypedDict(
    "SchemaTransformerType",
    {
        "type": str,
        # "mapping": t.Union[dict[str, t.Any], ExprType, list["SchemaTransformerType"], t.Any],
        "mapping": t.Any,
        "ctx": te.NotRequired[dict[str, t.Any]],
        "plugins": te.NotRequired[dict[str, t.Any]],
        "if": te.NotRequired[ExprType],
    },
)


def transformer_factory(
    schema: SchemaTransformerType,
    context: t.Optional[dict[str, t.Any]] = None,
    plugins: t.Optional[dict[str, t.Any]] = None,
):
    combinations = {
        MappingType.LITERAL: LiteralTransformer,
        MappingType.EXPR: ExprTransformer,
        MappingType.TUPLE: TupleTransformer,
        MappingType.LIST: ListTransformer,
        MappingType.OBJECT: ObjectTransformer,
    }
    return combinations[MappingType(schema["type"])](schema, context, plugins)


class Transformer:
    _schema: SchemaTransformerType
    _ctx: dict[str, t.Any]
    _plugins: dict[str, t.Any]
    _mapping: t.Any

    def __init__(
        self,
        schema: SchemaTransformerType,
        context: t.Optional[dict[str, t.Any]] = None,
        plugins: t.Optional[dict[str, t.Any]] = None,
    ) -> None:
        self._schema = schema
        self._ctx = {**self._schema.get("ctx", {}), **(context or {})}
        self._plugins = {**self._schema.get("plugins", {}), **(plugins or {})}
        self._mapping = self._schema["mapping"]


class LiteralTransformer(Transformer):
    _mapping: t.Any

    def __call__(self):
        return self._mapping


class TupleTransformer(Transformer):
    _mapping: list[SchemaTransformerType]

    def __call__(self) -> tuple[t.Any, ...]:
        return tuple(
            transformer_factory(st, self._ctx, self._plugins)()
            for st in self._mapping
            if not st.get("if")
            or execute_expr(t.cast(ExprType, st.get("if")), self._ctx, self._plugins)
        )


ListTransformerMappingType = t.TypedDict(
    "ListTransformerMappingType",
    {"iter": t.Union[str, ExprType], "each": SchemaTransformerType},
)


class ListTransformer(Transformer):
    _mapping: ListTransformerMappingType

    def __call__(self):
        iterable_def = self._mapping["iter"]
        iterable: t.Optional[Iterable[t.Any]] = None
        if isinstance(iterable_def, str):
            if iterable_def.startswith("$0."):
                iterable = self._ctx[iterable_def[3:]]
            elif iterable_def.startswith("$1."):
                plugin_path = iterable_def[3:]
                plugin_name, attr_name = plugin_path.split(":")
                iterable = getattr(self._plugins[plugin_name], attr_name)
        else:
            iterable = execute_expr(iterable_def, self._ctx, self._plugins)

        if not iterable:
            raise TypeError(f"invalid `iter` definition, {iterable_def} is not iterable")

        ret = []
        for loop_index, loop_item in enumerate(iterable):
            additional_ctx = {
                **self._ctx,
                "loop_index": loop_index,
                "loop_item": loop_item,
            }
            if not self._mapping["each"].get("if") or execute_expr(
                t.cast(ExprType, self._mapping["each"].get("if")),
                additional_ctx,
                self._plugins,
            ):
                val = transformer_factory(
                    self._mapping["each"], additional_ctx, self._plugins
                )()
                ret.append(val)

        return ret


class ExprTransformer(Transformer):
    _mapping: ExprType

    def __call__(self):
        return execute_expr(self._mapping, self._ctx, self._plugins)


class ObjectTransformer(Transformer):
    _mapping: dict[str, SchemaTransformerType]

    def __call__(self) -> dict[str, t.Any]:
        return {
            key: transformer_factory(st, self._ctx, self._plugins)()
            for key, st in self._mapping.items()
            if not st.get("if")
            or execute_expr(t.cast(ExprType, st.get("if")), self._ctx, self._plugins)
        }


def execute_expr(
    expr: list[t.Any],
    context:  t.Optional[dict[str, t.Any]] = None,
    plugins: t.Optional[dict[str, t.Any]] = None,
) -> t.Any:
    if not context:
        context = {}
    if not plugins:
        plugins = {}

    if not expr:
        raise TypeError("Expr can't be empty")
    if len(expr) == 1:
        arg = expr[0]
        if arg.startswith("$0."):
            ret = context[arg[3:]]
        elif arg.startswith("$1."):
            plugin_path = arg[3:]
            plugin_name, attr_name = plugin_path.split(":")
            ret = getattr(plugins[plugin_name], attr_name)
        else:
            raise TypeError(f"Invalid {expr}")
        return ret

    func: str = expr[0]
    args = expr[1:]

    eval_args = []
    for arg in args:
        if isinstance(arg, list):
            ret = execute_expr(arg, context, plugins)
            eval_args.append(ret)
        elif isinstance(arg, str):
            if arg.startswith("$0."):
                ret = context[arg[3:]]
                eval_args.append(ret)
            elif arg.startswith("$1."):
                plugin_path = arg[3:]
                plugin_name, attr_name = plugin_path.split(":")
                ret = getattr(plugins[plugin_name], attr_name)
                eval_args.append(ret)
            else:
                eval_args.append(arg)
        else:
            eval_args.append(arg)

    if func == "if":
        return eval_args[1] if eval_args[0] else eval_args[2]
    elif func.startswith("."):
        # it's first arg's method
        # otherwise it's a function
        try:
            _func = getattr(eval_args[0], func[1:])
        except AttributeError:
            raise FunctionNotFoundError(f"Method `{func[1:]}` not found in {args[0]}")

        eval_args.pop(0)
    elif func.startswith("$1."):
        plugin_path = func[3:]
        plugin_name, func_name = plugin_path.split(".")
        try:
            _func = getattr(plugins[plugin_name], func_name)
        except AttributeError:
            raise FunctionNotFoundError(
                f"Method `{func_name}` not found in plugin {plugin_name}"
            )
    else:
        try:
            _func = getattr(operator, func)
        except AttributeError:
            try:
                _func = getattr(builtins, func)
            except AttributeError:
                raise FunctionNotFoundError(f"Function `{func}` is not supported")
    return _func(*eval_args)
