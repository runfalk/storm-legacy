import ast
import pytest
import sys

from _pytest.assertion.rewrite import rewrite_asserts
from docutils.core import publish_doctree


def get_pytest_version_info():
    return tuple(map(int, pytest.__version__.split(".")[:2]))


def is_code_block(node):
    is_literal_block = node.tagname == "literal_block"
    return is_literal_block and "code" in node.attributes["classes"]


@pytest.mark.parametrize("path", [
    "doc/basic-usage.rst",
    "doc/infoheritance.rst",
])
def test_rst(path):
    with open(path) as f:
        rst = f.read()
        doctree = publish_doctree(rst)

    ast_parts = []
    for block in doctree.traverse(condition=is_code_block):
        raw_text = block.astext()
        num_lines = raw_text.count("\n") + 1
        node = ast.parse(raw_text, path)
        ast.increment_lineno(node, block.line - num_lines - 1)
        ast_parts.extend(node.body)

    if sys.version_info >= (3, 8):
        mod = ast.Module(body=ast_parts, type_ignores=[])
    else:
        mod = ast.Module(body=ast_parts)

    # Pytest 5 is Python 3 only and there are some API differences we need to
    # consider
    if get_pytest_version_info() < (5,):
        rewrite_asserts(mod, None)
    else:
        rewrite_asserts(mod, rst)
    exec(compile(mod, path, "exec"), {})
