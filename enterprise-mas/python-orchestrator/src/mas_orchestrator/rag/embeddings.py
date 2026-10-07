"""Embedding providers.

Embeddings are model output, so they go through the same discipline as any
other model call: an explicit interface, a named model on every vector, and no
silent fallback when the provider is unavailable.

The one implementation that ships here is a deterministic hashing embedder. It
is **not** a semantic model and does not pretend to be one: it exists so the
pipeline can be exercised end to end in development and tests without a
network. It refuses to be selected in production, because behind it a
"semantic search" would be returning bags of words.
"""

from __future__ import annotations

import hashlib
import logging
import math
import re
from typing import TYPE_CHECKING, Final, Protocol, runtime_checkable

from mas_orchestrator.config.enums import Environment
from mas_orchestrator.contracts.errors import (
    ConfigurationError,
    InvalidRequestError,
    ModelProviderError,
)
from mas_orchestrator.providers.base_http import HttpPolicy, HttpTransport, classify_status

if TYPE_CHECKING:
    from collections.abc import Sequence

    from mas_orchestrator.security.credentials import Secret

_LOGGER: Final[logging.Logger] = logging.getLogger(__name__)

_TOKEN: Final[re.Pattern[str]] = re.compile(r"[a-z0-9]+")


@runtime_checkable
class Embedder(Protocol):
    """Produces vectors for text."""

    @property
    def model(self) -> str:
        """Identifier of the embedding model, recorded with every vector."""
        ...

    @property
    def dimensions(self) -> int:
        """Vector width."""
        ...

    async def embed(self, texts: Sequence[str]) -> list[tuple[float, ...]]:
        """Embeds a batch of texts.

        Args:
            texts: The texts to embed.

        Returns:
            One vector per input, in the same order.
        """
        ...


class HashingEmbedder:
    """A deterministic bag-of-words embedder for development and tests.

    It captures lexical overlap, which is enough to exercise retrieval, and
    nothing more. Paraphrases will not match. That limitation is the reason it
    is refused in production.
    """

    def __init__(
        self,
        *,
        dimensions: int = 256,
        environment: Environment = Environment.DEVELOPMENT,
    ) -> None:
        if environment is Environment.PRODUCTION:
            raise ConfigurationError(
                "HashingEmbedder is not a semantic model and must not be used "
                "in production; configure a real embedding provider",
                details={"environment": environment.value},
            )
        if dimensions < 8:
            raise ConfigurationError(
                "embedding dimensions must be at least 8", details={"dimensions": dimensions}
            )
        self._dimensions = dimensions
        self._environment = environment
        self._model = f"hashing-{dimensions}"

    @property
    def model(self) -> str:
        """Model identifier."""
        return self._model

    @property
    def dimensions(self) -> int:
        """Vector width."""
        return self._dimensions

    async def embed(self, texts: Sequence[str]) -> list[tuple[float, ...]]:
        """Embeds texts deterministically."""
        return [self._embed_one(text) for text in texts]

    def _embed_one(self, text: str) -> tuple[float, ...]:
        """Builds one normalised bag-of-words vector."""
        vector = [0.0] * self._dimensions
        for token in _TOKEN.findall(text.lower()):
            digest = hashlib.sha256(token.encode("utf-8")).digest()
            index = int.from_bytes(digest[:4], "big") % self._dimensions
            # Sign-bit hashing reduces collision bias at the cost of a little
            # sparsity, which is the usual trade for this construction.
            sign = 1.0 if digest[4] & 1 else -1.0
            vector[index] += sign
        return _normalise(vector)


def _normalise(vector: list[float]) -> tuple[float, ...]:
    """Scales a vector to unit length.

    Unit length makes the dot product a cosine similarity, so scores are
    comparable across documents of different lengths.
    """
    magnitude = math.sqrt(sum(value * value for value in vector))
    if magnitude == 0.0:
        return tuple(vector)
    return tuple(value / magnitude for value in vector)


def cosine_similarity(left: Sequence[float], right: Sequence[float]) -> float:
    """Cosine similarity between two vectors.

    Raises:
        ConfigurationError: If the vectors have different widths. Comparing
            vectors from two different embedding models produces a number that
            looks meaningful and is not, so it is refused outright.
    """
    if len(left) != len(right):
        raise ConfigurationError(
            "cannot compare embeddings of different dimensions",
            details={"left": len(left), "right": len(right)},
        )
    total = sum(a * b for a, b in zip(left, right, strict=True))
    return total


class EmbedderUnavailable:
    """An embedder that always fails.

    Used when no embedding provider is configured. It exists so the pipeline
    fails with a clear configuration error at the embedding step rather than
    producing zero vectors, which would make every document equally irrelevant
    and retrieve essentially at random.
    """

    @property
    def model(self) -> str:
        """Model identifier."""
        return "unavailable"

    @property
    def dimensions(self) -> int:
        """Vector width."""
        return 0

    async def embed(self, texts: Sequence[str]) -> list[tuple[float, ...]]:
        """Always raises."""
        raise ConfigurationError(
            "no embedding provider is configured",
            details={"texts": len(texts)},
        )


class OpenAIEmbedder:
    """Real OpenAI-compatible embeddings over the guarded HTTP transport.

    The endpoint is fixed to ``api.openai.com``; neither request content nor
    configuration can choose a host. Provider responses are shape-, count-,
    dimension-, and finiteness-checked before vectors reach the index.
    """

    def __init__(
        self,
        *,
        api_key: Secret,
        model: str,
        dimensions: int,
        request_timeout_seconds: float = 30.0,
        max_batch_size: int = 64,
    ) -> None:
        if not model.strip() or len(model) > 128:
            raise ConfigurationError("embedding model name is invalid", details={"field": "model"})
        if dimensions != 1536:
            raise ConfigurationError(
                "the current pgvector plane requires 1536-dimensional embeddings",
                details={"dimensions": dimensions},
            )
        if not 1 <= max_batch_size <= 256:
            raise ConfigurationError(
                "embedding batch size is outside the supported range",
                details={"max_batch_size": max_batch_size},
            )
        self._model = model
        self._dimensions = dimensions
        self._max_batch_size = max_batch_size
        self._transport = HttpTransport(
            HttpPolicy(
                allowed_hosts=frozenset({"api.openai.com"}),
                max_response_bytes=8 * 1024 * 1024,
                max_redirects=0,
            ),
            connect_timeout_seconds=5.0,
            read_timeout_seconds=request_timeout_seconds,
            write_timeout_seconds=request_timeout_seconds,
            credential=api_key,
        )

    @property
    def model(self) -> str:
        """Configured embedding model identifier."""
        return self._model

    @property
    def dimensions(self) -> int:
        """Configured vector width."""
        return self._dimensions

    async def aclose(self) -> None:
        """Closes the guarded provider transport."""
        await self._transport.aclose()

    async def embed(self, texts: Sequence[str]) -> list[tuple[float, ...]]:
        """Embeds one bounded batch, preserving the input order."""
        if not texts:
            return []
        if len(texts) > self._max_batch_size:
            raise InvalidRequestError(
                "embedding request exceeds the configured batch size",
                details={"limit": self._max_batch_size, "received": len(texts)},
            )
        if any(not text.strip() or len(text.encode("utf-8")) > 64 * 1024 for text in texts):
            raise InvalidRequestError(
                "embedding input is empty or exceeds the per-item byte limit",
                details={"maximum_bytes": 64 * 1024},
            )

        response = await self._transport.request(
            "POST",
            "https://api.openai.com/v1/embeddings",
            json_body={
                "model": self._model,
                "input": list(texts),
                "dimensions": self._dimensions,
                "encoding_format": "float",
            },
            headers={"content-type": "application/json"},
        )
        if not 200 <= response.status_code < 300:
            raise classify_status(response.status_code)
        payload = response.json()
        if not isinstance(payload, dict) or not isinstance(payload.get("data"), list):
            raise ModelProviderError("embedding provider returned an invalid response")
        records = payload["data"]
        if len(records) != len(texts):
            raise ModelProviderError(
                "embedding provider returned a different number of vectors than requested",
                details={"expected": len(texts), "received": len(records)},
            )

        ordered: list[tuple[float, ...] | None] = [None] * len(texts)
        for record in records:
            if not isinstance(record, dict):
                raise ModelProviderError("embedding provider returned an invalid vector record")
            index = record.get("index")
            values = record.get("embedding")
            if (
                not isinstance(index, int)
                or isinstance(index, bool)
                or not 0 <= index < len(texts)
                or not isinstance(values, list)
                or ordered[index] is not None
            ):
                raise ModelProviderError("embedding provider returned an invalid vector index")
            if len(values) != self._dimensions or any(
                not isinstance(value, (int, float))
                or isinstance(value, bool)
                or not math.isfinite(float(value))
                for value in values
            ):
                raise ModelProviderError(
                    "embedding provider returned a vector with invalid dimensions or values",
                    details={"expected_dimensions": self._dimensions},
                )
            ordered[index] = tuple(float(value) for value in values)
        if any(vector is None for vector in ordered):
            raise ModelProviderError("embedding provider omitted a requested vector")
        return [vector for vector in ordered if vector is not None]
