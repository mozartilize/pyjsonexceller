import pytest

from pyjsonexceller.exceptions import FunctionNotFound, PluginDefinitionError, PluginNotFound
from pyjsonexceller.transform import (
    ExprTransformer,
    ListTransformer,
    LiteralTransformer,
    ObjectTransformer,
    TupleTransformer,
    execute_expr,
    transformer_factory,
)


class FooPlugin:
    pass


foo_plugin = FooPlugin()


@pytest.mark.parametrize(
    "expr,context,ret",
    [
        (["lt", 10, 11], None, True),
        ([".__lt__", 10, 11], None, True),
        (["concat", "10", "11"], None, "1011"),
        (
            ["if", ["getitem", "$0.rec", "val"], "left", "right"],
            {"rec": {"val": True}},
            "left",
        ),
        (
            ["getitem", ["getitem", "$0.rec", "foo"], "foo"],
            {
                "rec": {
                    "foo": {"foo": 1},
                }
            },
            1,
        ),
        (
            ["$0.rec"],
            {
                "rec": {
                    "foo": {"foo": 1},
                }
            },
            {
                "foo": {"foo": 1},
            },
        ),
        (
            [
                "if",
                ["getitem", "$0.rec", "val"],
                ["if", ["getitem", "$0.rec", "val"], "inner_left", "inner_right"],
                "right",
            ],
            {"rec": {"val": True}},
            "inner_left",
        ),
    ],
)
def test_execute_expr(expr, context, ret):
    r = execute_expr(expr, context)
    assert r == ret


@pytest.mark.parametrize(
    "mapping", ["hello", 1, 1.5, True, ["hello", 1, 1.5, True], {"foo": "bar"}]
)
def test_literal(mapping):
    schema = {"type": "literal", "mapping": mapping}
    t = LiteralTransformer(schema)
    assert t() == mapping


def test_expr():
    schema = {
        "type": "expr",
        "mapping": ["$0.rec"],
        "ctx": {
            "rec": {
                "foo": {"foo": 1},
            }
        },
    }

    t = ExprTransformer(schema)
    assert t() == {
        "foo": {"foo": 1},
    }


def test_tuple():
    schema = {
        "type": "tuple",
        "mapping": [
            {
                "type": "object",
                "mapping": {"id": {"type": "literal", "mapping": "hello"}},
            }
        ],
    }

    t = TupleTransformer(schema)
    assert t() == ({"id": "hello"},)


def test_list():
    schema = {
        "type": "list",
        "mapping": {
            "iter": ["$0.rec"],
            "each": {
                "type": "object",
                "mapping": {
                    "id": {
                        "type": "expr",
                        "mapping": ["concat", "id_", ["str", "$0.loop_index"]],
                    },
                    "val": {"type": "expr", "mapping": ["$0.loop_item"]},
                },
            },
        },
        "ctx": {"rec": [1, 2, 3, 4]},
    }

    t = ListTransformer(schema)
    assert t() == [
        {
            "id": "id_0",
            "val": 1,
        },
        {
            "id": "id_1",
            "val": 2,
        },
        {
            "id": "id_2",
            "val": 3,
        },
        {
            "id": "id_3",
            "val": 4,
        },
    ]


def test_list_errors():
    schema = {
        "type": "list",
        "mapping": {
            "iter": ["$0.rec"],
            "each": {
                "type": "object",
                "mapping": {
                    "id": {
                        "type": "expr",
                        "mapping": ["concat", "id_", ["str", "$0.loop_index"]],
                    },
                    "val": {"type": "expr", "mapping": ["$0.loop_item"]},
                },
            },
        },
        "ctx": {"rec": 1},
    }

    with pytest.raises(TypeError) as exec_info:
        t = ListTransformer(schema)
        t()
    assert (
        exec_info.value.args[0]
        == "invalid `iter` definition, ['$0.rec'] is not iterable"
    )


def test_object():
    schema = {
        "type": "object",
        "mapping": {"id": {"type": "literal", "mapping": "hello"}},
    }

    t = ObjectTransformer(schema)
    assert t() == {"id": "hello"}


@pytest.mark.parametrize(
    "plugins,expr",
    [
        (["datetime:datetime"], ["$1.datetime:strptime", "$0.datestr", "%Y-%m-%d"]),
        (
            [{"datetime": "datetime:datetime"}],
            ["$1.datetime:strptime", "$0.datestr", "%Y-%m-%d"],
        ),
        (
            ["datetime", {"datetime": ["$1.datetime:datetime"]}],
            ["$1.datetime:strptime", "$0.datestr", "%Y-%m-%d"],
        ),
        (
            ["datetime", ["$1.datetime:datetime"]],
            ["$1.datetime:strptime", "$0.datestr", "%Y-%m-%d"],
        ),
        (
            ["datetime", ["getattr", "$1.datetime", "datetime"]],
            [["getattr", "$1.datetime", "strptime"], "$0.datestr", "%Y-%m-%d"],
        ),
    ],
)
def test_plugins(plugins, expr):
    schema = {
        "type": "expr",
        "mapping": expr,
        "ctx": {"datestr": "2024-12-02"},
        "plugins": plugins,
    }

    t = ExprTransformer(schema)
    from datetime import datetime

    assert t() == datetime(2024, 12, 2)


@pytest.mark.parametrize(
    "schema,exec,args",
    [
        ({"type": "expr", "mapping": ["$1.foo", "abcd"]}, PluginNotFound, ("foo",)),
        (
            {"type": "expr", "mapping": ["$1.json:foo", "abcd"], "plugins": ["json"]},
            AttributeError,
            ("No attribute `foo` in plugin `json:foo`",),
        ),
        (
            {"type": "expr", "mapping": ["$1.foo", "abcd"], "plugins": ["foo"]},
            PluginDefinitionError,
            ("No module/package `foo` installed",),
        ),
        (
            {"type": "expr", "mapping": ["$1.foo", "abcd"], "plugins": [1]},
            PluginDefinitionError,
            ("1",),
        ),
        (
            {
                "type": "expr",
                "mapping": ["$1.foo", "abcd"],
                "plugins": [{"foo": "foo"}],
            },
            PluginDefinitionError,
            ("No module/package `foo` installed",),
        ),
        (
            {
                "type": "expr",
                "mapping": ["$1.foo", "abcd"],
                "plugins": [{"foo": "json:foo"}],
            },
            PluginDefinitionError,
            ("No attribute `foo` on module/package `json`",),
        ),
        (
            {"type": "expr", "mapping": ["$1.foo", "abcd"], "plugins": [{"foo": 1}]},
            PluginDefinitionError,
            ("{'foo': 1}",),
        ),
        (
            {
                "type": "expr",
                "mapping": ["$1.foo:bar"],
                "plugins": [
                    f"{__name__}:foo_plugin",
                    {"foo": ["getattr", "$1.foo_plugin", "foo"]},
                ],
            },
            PluginDefinitionError,
            ("No attribute `foo` on module/package `foo_plugin`",),
        ),
    ],
)
def test_plugin_errors(schema, exec, args):
    with pytest.raises(exec) as e:
        t = transformer_factory(schema)
        t()
    assert args == e.value.args


@pytest.mark.parametrize(
    "schema,exec,args",
    [
        ({"type": "expr", "mapping": []}, TypeError, ("Expr can't be empty",)),
        ({"type": "expr", "mapping": ["$0.foo"]}, AttributeError, ("No attribute `foo` in context",)),
        ({"type": "expr", "mapping": [".bar", 1]}, FunctionNotFound, ("Method `bar` not found in 1",)),
        ({"type": "expr", "mapping": ["bar", 1]}, FunctionNotFound, ("Function `bar` is not supported",)),
    ],
)
def test_expr_errors(schema, exec, args):
    with pytest.raises(exec) as e:
        t = transformer_factory(schema)
        t()
    assert args == e.value.args
