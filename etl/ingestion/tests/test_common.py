from datetime import datetime
from typing import Any, Dict, List

from src.common import add_column


def test_add_column():
    data: List[Dict[str, Any]] = [
        {"login": "octocat", "name": "The Octocat"},
        {"login": "rbojan2000", "name": "Bojan Radovic"},
    ]

    ts = int(datetime(2025, 11, 25).timestamp())
    result = add_column(data, "ingested_at", ts)

    expected: List[Dict[str, Any]] = [
        {"login": "octocat", "name": "The Octocat", "ingested_at": ts},
        {"login": "rbojan2000", "name": "Bojan Radovic", "ingested_at": ts},
    ]

    assert result == expected
