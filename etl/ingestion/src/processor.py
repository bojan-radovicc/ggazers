import logging
import time
from datetime import datetime, timezone
from typing import Any, Dict, List

from src.common import add_column
from src.git_clients.gh_archive_client import GHArchiveClient
from src.git_clients.github_client import GithubClient
from src.hdfs_client import HDFSClient
from src.paths import ACTORS_PATH, GITHUB_EVENTS_PATH, REPOS_PATH
from src.utils import (
    chunk_list,
    decompress_data,
    extract_repos_and_actors,
    generate_file_name,
)

logger = logging.getLogger(__name__)


class Processor:
    def __init__(
        self, gh_archive_client: GHArchiveClient, github_client: GithubClient, hdfs_client: HDFSClient
    ) -> None:
        self.gh_archive_client = gh_archive_client
        self.github_client = github_client
        self.hdfs_client = hdfs_client

    def _ingest_repos(self, repos: List[str], chunk_size: int, sleep_on_failure: int) -> List[Dict[Any, Any]]:
        for chunk_index, chunk in enumerate(chunk_list(repos, chunk_size)):
            logger.info(f"Processing repo chunk: #{chunk_index}")
            query = self.github_client.build_graphql_query(repos=chunk)
            data = self.github_client.run_query(query)
            if not data:
                logger.info("GraphQL query failed, trying REST API...")
                data = self.github_client.hit_rest_api("repos", chunk)

            if not data:
                logger.warning("All authentication attempts failed. Sleeping for 4 hours...")
                time.sleep(sleep_on_failure)

            return data

    def _ingest_actors(
        self, actors: List[str], chunk_size: int, sleep_on_failure: int
    ) -> List[Dict[Any, Any]]:
        for chunk_index, chunk in enumerate(chunk_list(actors, chunk_size)):
            logger.info(f"Processing actor chunk: #{chunk_index}")
            query = self.github_client.build_graphql_query(actors=chunk)
            data = self.github_client.run_query(query)
            if not data:
                logger.info("GraphQL query failed, trying REST API...")
                data = self.github_client.hit_rest_api("users", chunk)

            if not data:
                logger.warning("All authentication attempts failed. Sleeping for 4 hours...")
                time.sleep(sleep_on_failure)

            return data

    def run(self, start_date: str, end_date: str, chunk_size: int, sleep_on_failure: int) -> None:
        logger.info(f"Starting github events ingestion for period: {start_date} to {end_date}")

        for daily_part_data, current_date, part in self.gh_archive_client.get_events_dump(
            start_date, end_date
        ):
            data = decompress_data(daily_part_data)
            data = add_column(
                list=data, column_name="ingested_at", value=int(datetime.now(timezone.utc).timestamp())
            )
            filename = generate_file_name(current_date, part)
            filepath = f"{GITHUB_EVENTS_PATH}/{current_date.strftime('%Y_%m_%d')}/{filename}"
            self.hdfs_client.write_jsonl(data, filepath)

            repos, actors = extract_repos_and_actors(data)
            logger.info(f"Extracted {len(repos)} unique repos and {len(actors)} unique actors.")

            repos = self._ingest_repos(repos, chunk_size, sleep_on_failure)
            repos = add_column(
                list=repos,
                column_name="ingested_at",
                value=int(datetime.now(timezone.utc).timestamp()),
            )

            filename = generate_file_name(current_date, part)
            filepath = f"{REPOS_PATH}/{current_date.strftime('%Y_%m_%d')}/{filename}"
            self.hdfs_client.write_jsonl(repos, filepath)

            actors = self._ingest_actors(actors, chunk_size, sleep_on_failure)
            actors = add_column(
                list=actors,
                column_name="ingested_at",
                value=int(datetime.now(timezone.utc).timestamp()),
            )

            filename = generate_file_name(current_date, part)
            filepath = f"{ACTORS_PATH}/{current_date.strftime('%Y_%m_%d')}/{filename}"
            self.hdfs_client.write_jsonl(actors, filepath)
