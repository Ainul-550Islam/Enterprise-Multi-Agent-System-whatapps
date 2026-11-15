"""mas SDK error tests."""

import pytest
from mas_sdk.errors import MasError, ApiError, ValidationError, ErrorCode


def test_mas_error():
    err = MasError("TEST_CODE", "test message")
    assert err.code == "TEST_CODE"
    assert err.message == "test message"
    assert str(err) == "[TEST_CODE] test message"


def test_api_error():
    err = ApiError("VALIDATION", "validation failed")
    assert err.code == "VALIDATION"
    assert isinstance(err, MasError)


def test_validation_error():
    err = ValidationError("invalid input", "field is required")
    assert err.code == "VALIDATION"
    assert isinstance(err, ApiError)


def test_error_code_constants():
    assert ErrorCode.TRANSPORT == "TRANSPORT"
    assert ErrorCode.RATE_LIMIT == "RATE_LIMIT"
    assert ErrorCode.NOT_FOUND == "NOT_FOUND"
    assert ErrorCode.VALIDATION == "VALIDATION"
