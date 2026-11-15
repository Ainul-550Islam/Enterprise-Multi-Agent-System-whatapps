"""mas SDK - Error types and mapping."""

from __future__ import annotations

from typing import Optional, Dict, Any


class MasError(Exception):
    """Base mas error."""
    def __init__(self, code: str, message: str, details: Optional[Dict[str, Any]] = None) -> None:
        self.code = code
        self.message = message
        self.details = details or {}
        super().__init__(f"[{code}] {message}")


class ApiError(MasError):
    """API-level error from the mas service."""
    pass


class ValidationError(ApiError):
    """Validation error from the mas service."""
    code = "VALIDATION"
    def __init__(self, message: str, details: Optional[dict] = None) -> None:
        super().__init__(self.code, message, details)


class ErrorCode:
    """Error code constants."""
    
    # Client/transport errors
    TRANSPORT = "TRANSPORT"
    PARSE = "PARSE"
    TIMEOUT = "TIMEOUT"
    CONNECT = "CONNECT"
    
    # API errors
    VALIDATION = "VALIDATION"
    AUTH = "AUTH"
    NOT_FOUND = "NOT_FOUND"
    CONFLICT = "CONFLICT"
    RATE_LIMIT = "RATE_LIMIT"
    SERVER_ERROR = "SERVER_ERROR"
    
    # Domain errors
    AGENT_NOT_FOUND = "AGENT_NOT_FOUND"
    WORKFLOW_NOT_FOUND = "WORKFLOW_NOT_FOUND"
    KEY_NOT_FOUND = "KEY_NOT_FOUND"
    
    @classmethod
    def all_codes(cls) -> list[str]:
        return [
            cls.TRANSPORT, cls.PARSE, cls.TIMEOUT, cls.CONNECT,
            cls.VALIDATION, cls.AUTH, cls.NOT_FOUND, cls.CONFLICT,
            cls.RATE_LIMIT, cls.SERVER_ERROR,
            cls.AGENT_NOT_FOUND, cls.WORKFLOW_NOT_FOUND, cls.KEY_NOT_FOUND,
        ]


def map_http_status(status: int, body: dict) -> ApiError:
    """Map an HTTP response to an ApiError."""
    code = body.get("code", "UNKNOWN")
    message = body.get("message", "Unknown error")
    
    # Handle known status ranges
    if 400 <= status < 500:
        if status == 401:
            return ApiError(ErrorCode.AUTH, message)
        elif status == 403:
            return ApiError(ErrorCode.AUTH, message)
        elif status == 404:
            return ApiError(ErrorCode.NOT_FOUND, message)
        elif status == 409:
            return ApiError(ErrorCode.CONFLICT, message)
        elif status == 422:
            return ApiError(ErrorCode.VALIDATION, message)
        elif status == 429:
            return ApiError(ErrorCode.RATE_LIMIT, message)
    elif 500 <= status < 600:
        return ApiError(ErrorCode.SERVER_ERROR, message)
    
    return ApiError(code, message)
