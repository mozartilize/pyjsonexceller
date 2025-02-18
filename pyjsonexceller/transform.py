import builtins
import operator
import types
import typing as t
from abc import abstractmethod
from collections.abc import Iterable
from enum import Enum
from importlib import import_module

import typing_extensions as te

from pyjsonexceller.exceptions import (
    FunctionNotFound,
    PluginDefinitionError,
    PluginNotFound,
)


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
        "mapping": t.Any,
        "ctx": te.NotRequired[dict[str, t.Any]],
        "plugins": te.NotRequired[dict[str, t.Any]],
        "if": te.NotRequired[ExprType],
        "computed": te.NotRequired[dict[str, "SchemaTransformerType"]],
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


def _dynamic_import(definition: str):
    if ":" in definition:
        module_path, attrname = definition.split(":")
    else:
        module_path = definition
        attrname = None
    attr = import_module(module_path)
    if attrname:
        attr = getattr(attr, attrname)
        if not hasattr(attr, "__name__"):
            attr.__name__ = attrname
    return attr


class Transformer:
    _schema: SchemaTransformerType
    _ctx: dict[str, t.Any]
    _plugins: dict[str, t.Union[t.Callable[..., t.Any], types.ModuleType]]
    _mapping: t.Any
    _computed: dict[str, t.Any]

    def __init__(
        self,
        schema: SchemaTransformerType,
        context: t.Optional[dict[str, t.Any]] = None,
        plugins: t.Optional[
            dict[str, t.Union[t.Callable[..., t.Any], types.ModuleType]]
        ] = None,
    ) -> None:
        self._schema = schema
        self._ctx = {**self._schema.get("ctx", {}), **(context or {})}
        self._plugins = self._load_plugins(plugins or {})
        self._mapping = self._schema["mapping"]
        self._computed = {}
        self._computed_resolved: bool = False

    def _resolve_computed(self):
        if self._schema.get("computed"):
            self._computed = {
                var: transformer_factory(st, self._ctx, self._plugins)()
                for var, st in t.cast(
                    dict[str, "SchemaTransformerType"], self._schema.get("computed")
                ).items()
            }
        self._computed_resolved = True

    def _load_plugins(
        self,
        additional_plugins: dict[
            str, t.Union[t.Callable[..., t.Any], types.ModuleType]
        ],
    ):
        schema_plugins = {**(additional_plugins or {})}
        try:
            for plugin_def in self._schema.get("plugins", []):
                if isinstance(plugin_def, str):
                    plugin = _dynamic_import(plugin_def)
                    schema_plugins[plugin.__name__] = plugin
                elif isinstance(plugin_def, list):
                    plugin = execute_expr(plugin_def, None, schema_plugins)
                    schema_plugins[plugin.__name__] = plugin
                elif isinstance(plugin_def, dict):
                    plugin_name: str = tuple(plugin_def.keys())[0]
                    if isinstance(plugin_def[plugin_name], list):
                        plugin = execute_expr(
                            plugin_def[plugin_name], None, schema_plugins
                        )
                    elif isinstance(plugin_def[plugin_name], str):
                        plugin = _dynamic_import(plugin_def[plugin_name])
                    else:
                        raise PluginDefinitionError(f"{plugin_def}")
                    schema_plugins[plugin_name] = plugin
                else:
                    raise PluginDefinitionError(f"{plugin_def}")
        except ModuleNotFoundError as e:
            raise PluginDefinitionError(f"No module/package `{e.name}` installed")
        except AttributeError as e:
            raise PluginDefinitionError(
                f"No attribute `{e.name}` on module/package `{e.obj.__name__}`"
            )
        return schema_plugins

    @abstractmethod
    def _resolve(self) -> t.Any:
        raise NotImplementedError()  # pragma: no cover

    def __call__(self) -> t.Any:
        if not self._computed_resolved:
            self._resolve_computed()
            self._ctx.update(self._computed)
        return self._resolve()


class LiteralTransformer(Transformer):
    _mapping: t.Any

    def _resolve(self):
        return self._mapping


class TupleTransformer(Transformer):
    _mapping: list[SchemaTransformerType]

    def _resolve(self) -> tuple[t.Any, ...]:
        return tuple(
            transformer_factory(st, self._ctx, self._plugins)()
            for st in self._mapping
            if not st.get("if")
            or execute_expr(t.cast(ExprType, st.get("if")), self._ctx, self._plugins)
        )


ListTransformerMappingType = t.TypedDict(
    "ListTransformerMappingType",
    {"iter": ExprType, "each": SchemaTransformerType},
)


class ListTransformer(Transformer):
    _mapping: ListTransformerMappingType

    def _resolve(self):
        iterable_expr = self._mapping["iter"]
        iterable = execute_expr(iterable_expr, self._ctx, self._plugins)

        if not isinstance(iterable, Iterable):
            raise TypeError(
                f"invalid `iter` definition, {iterable_expr} is not iterable"
            )

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

    def _resolve(self):
        return execute_expr(self._mapping, self._ctx, self._plugins)


class ObjectTransformer(Transformer):
    _mapping: dict[str, SchemaTransformerType]

    def _resolve(self) -> dict[str, t.Any]:
        return {
            key: transformer_factory(st, self._ctx, self._plugins)()
            for key, st in self._mapping.items()
            if not st.get("if")
            or execute_expr(t.cast(ExprType, st.get("if")), self._ctx, self._plugins)
        }


def _resolve_plugin(plugin_path, plugins):
    try:
        if ":" in plugin_path:
            plugin_name, attrname = plugin_path.split(":")
        else:
            plugin_name = plugin_path
            attrname = None
        ret = plugins[plugin_name]
        if attrname:
            ret = getattr(ret, attrname)
    except KeyError as e:
        raise PluginNotFound(f"{e.args[0]}")
    except AttributeError as e:
        raise AttributeError(
            f"No attribute `{e.name}` in plugin `{plugin_path}`", name=e.name
        )
    return ret


def _resolve_arg(arg, context, plugins):
    if isinstance(arg, list):
        ret = execute_expr(arg, context, plugins)
    elif isinstance(arg, str):
        if arg.startswith("$0."):
            try:
                ret = context[arg[3:]]
            except KeyError as e:
                raise AttributeError(
                    f"No attribute `{e.args[0]}` in context", name=e.args[0]
                )
        elif arg.startswith("$1."):
            plugin_path = arg[3:]
            ret = _resolve_plugin(plugin_path, plugins)
        else:
            ret = arg
    else:
        ret = arg
    return ret


def execute_expr(
    expr: list[t.Any],
    context: t.Optional[dict[str, t.Any]] = None,
    plugins: t.Optional[dict[str, t.Any]] = None,
) -> t.Any:
    if not context:
        context = {}
    if not plugins:
        plugins = {}

    if not expr:
        raise TypeError("Expr can't be empty")
    if len(expr) == 1:
        return _resolve_arg(expr[0], context, plugins)

    eval_args = []
    for arg in expr[1:]:
        eval_args.append(_resolve_arg(arg, context, plugins))

    func = expr[0]

    if func == "if":
        return eval_args[1] if eval_args[0] else eval_args[2]
    elif isinstance(func, list):
        _func = execute_expr(func, None, plugins)
    elif func.startswith("."):
        # it's first arg's method
        # otherwise it's a function
        try:
            _func = getattr(eval_args[0], func[1:])
            eval_args.pop(0)
        except AttributeError:
            raise FunctionNotFound(f"Method `{func[1:]}` not found in {expr[1]}")

    elif func.startswith("$1."):
        plugin_path = func[3:]
        _func = _resolve_plugin(plugin_path, plugins)
    else:
        try:
            _func = getattr(operator, func)
        except AttributeError:
            try:
                _func = getattr(builtins, func)
            except AttributeError:
                raise FunctionNotFound(f"Function `{func}` is not supported")
    return _func(*eval_args)
