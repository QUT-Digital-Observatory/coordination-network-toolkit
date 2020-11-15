import pytest

from coordination_network_toolkit.similarity import MinDocSizeSimilarity, similarity


@pytest.mark.parametrize(
    ["text_pair", "result"],
    [
        (("hello world", "hello world"), 1),
        (("hello world", "world hello"), 1),
    ]
)
def test_similarity(text_pair, result):
    assert similarity(*text_pair) == result
