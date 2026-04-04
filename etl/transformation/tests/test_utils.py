from datetime import date

from src.paths import ACTORS_PATH, REPOS_PATH
from src.utils import build_paths


def test_build_paths_single_day_actors() -> None:
    start_date = date(2025, 11, 1)
    end_date = date(2025, 11, 1)

    paths = build_paths(start_date, end_date, "actors")

    assert paths == [f"{ACTORS_PATH}/2025_11_01/*.jsonl"]


def test_build_paths_single_day_repos() -> None:
    start_date = date(2025, 11, 1)
    end_date = date(2025, 11, 1)

    paths = build_paths(start_date, end_date, "repos")

    assert paths == [f"{REPOS_PATH}/2025_11_01/*.jsonl"]


def test_build_paths_multiple_days() -> None:
    start_date = date(2025, 11, 1)
    end_date = date(2025, 11, 3)

    paths = build_paths(start_date, end_date, "actors")

    assert len(paths) == 3
    assert paths[0] == f"{ACTORS_PATH}/2025_11_01/*.jsonl"
    assert paths[1] == f"{ACTORS_PATH}/2025_11_02/*.jsonl"
    assert paths[2] == f"{ACTORS_PATH}/2025_11_03/*.jsonl"


def test_build_paths_custom_parts_per_date() -> None:
    start_date = date(2025, 11, 1)
    end_date = date(2025, 11, 1)

    paths = build_paths(start_date, end_date, "actors", parts_per_date=10)

    assert paths == [f"{ACTORS_PATH}/2025_11_01/*.jsonl"]


def test_build_paths_month_boundary() -> None:
    start_date = date(2025, 1, 31)
    end_date = date(2025, 2, 1)

    paths = build_paths(start_date, end_date, "repos")

    assert len(paths) == 2
    assert paths[0] == f"{REPOS_PATH}/2025_01_31/*.jsonl"
    assert paths[1] == f"{REPOS_PATH}/2025_02_01/*.jsonl"
