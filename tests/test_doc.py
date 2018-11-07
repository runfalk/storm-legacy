import ast
import pytest

from _pytest.assertion.rewrite import AssertionRewriter
from docutils.core import publish_doctree


def is_code_block(node):
    is_literal_block = node.tagname == "literal_block"
    return is_literal_block and "code" in node.attributes["classes"]


@pytest.mark.parametrize("path", [
    "doc/basic-usage.rst",
    "doc/infoheritance.rst",
])
def test_rst(path):
    with open(path) as f:
        doctree = publish_doctree(f.read())

    ast_parts = []
    for block in doctree.traverse(condition=is_code_block):
        raw_text = block.astext()
        num_lines = raw_text.count("\n") + 1
        node = ast.parse(raw_text, path)
        ast.increment_lineno(node, block.line - num_lines - 1)
        ast_parts.extend(node.body)

    mod = ast.Module(body=ast_parts)
    AssertionRewriter(path, None).run(mod)
    exec(compile(mod, path, "exec"), {})
