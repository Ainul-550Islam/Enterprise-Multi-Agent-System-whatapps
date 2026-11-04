"""Stable Data Hub source kinds shared with the Rust execution bridge."""

import pytest

from mas_orchestrator.config.enums import DataSourceKind


def test_source_kind_wire_contract_is_closed() -> None:
    assert {kind.value for kind in DataSourceKind} == {"upload", "object_storage", "web"}
    for value in ("upload", "object_storage", "web"):
        assert DataSourceKind(value).value == value
    for invalid in ("UPLOAD", "filesystem", "", "unknown"):
        with pytest.raises(ValueError):
            DataSourceKind(invalid)
