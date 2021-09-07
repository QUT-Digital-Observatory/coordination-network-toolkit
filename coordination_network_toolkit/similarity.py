import regex
from typing import Callable, Pattern


word_tokenizer = regex.compile(
    # Note this handles 'quoted' words a little weirdly: 'orange' is tokenised
    # as ["orange", "'"] I'd prefer to tokenise this as ["'", "orange", "'"]
    # but the regex library behaves weirdly. So for now phrase search for
    # quoted strings won't work.
    r"\b\p{Word_Break=WSegSpace}*'?",
    flags=regex.WORD | regex.UNICODE | regex.V1,
)


social_media_cleaner = regex.compile(
    # Find strings that start with @ (ie, mentions)
    # And grab everything from the trailing slash up to but not including the next
    # whitespace character or end of text
    r"@.*?(?=(?:\s|$))"
)


def message_preprocessor(text: str):
    """A default preprocessing function for social media strings.

    This transforms the text to make the matching process invariant to
    some non-semantic transformations.

    This default:

    - strips @mentions
    - normalises some whitespace
    - lowercases the text

    """
    return " ".join(social_media_cleaner.sub("", text.lower()).split())


def tokenize(text: str, tokenizer: Pattern = word_tokenizer) -> str:
    words = sorted(
        set(t for t in tokenizer.split(social_media_cleaner.sub("", text.lower())) if t)
    )
    tokenized = " ".join(words)

    return tokenized


def similarity(tokens_1, tokens_2):
    set_1 = set(tokens_1.split())
    set_2 = set(tokens_2.split())
    return len(set_1 & set_2) / len(set_1 | set_2)


class MinDocSizeSimilarity:
    """
    A callable class for document similarity that discards short documents.

    This is designed to avoid considering extremely short documents (such as a tweet
    containing only a single mention and hashtag) as similar in any way.

    Note that this is a callable class rather than a function to make future parallel
    processing easier.
    """

    def __init__(self, min_tokens=5):
        self.min_tokens = min_tokens

    def __call__(self, tokens_1, tokens_2) -> float:

        set_1 = set(tokens_1.split())
        if len(set_1) < self.min_tokens:
            return 0

        set_2 = set(tokens_2.split())
        if len(set_2) < self.min_tokens:
            return 0

        return len(set_1 & set_2) / len(set_1 | set_2)


def get_similarity_fn_from_min_size(min_document_size_similarity) -> Callable:
    if min_document_size_similarity > 0:
        return MinDocSizeSimilarity(min_document_size_similarity)
    else:
        return similarity
