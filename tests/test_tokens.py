import pytest

from coordination_network_toolkit.similarity import tokenize


# todo: does order matter?

tokens_tests = [
    ("hello", "hello"),
    ("The quick brown fox jumped over the lazy dog.", ". brown dog fox jumped lazy over quick the"),
    ("", "")
]


@pytest.mark.parametrize(["text", "tokens"], tokens_tests)
def test_tokenize(text, tokens):
    result = tokenize(text)
    assert result == tokens
