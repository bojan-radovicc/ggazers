import json
from typing import Any, Dict, List

from hdfs import InsecureClient


class HDFSClient:
    def __init__(self, namenode_host: str = "namenode", hdfs_port: int = 9870, user: str = "ggazer") -> None:
        self.client: InsecureClient = InsecureClient(f"http://{namenode_host}:{hdfs_port}", user=user)

    def write_jsonl(self, data: List[Dict[str, Any]], filepath: str) -> None:
        if not self.client.status(filepath.rsplit("/", 1)[0], strict=False):
            self.client.makedirs(filepath.rsplit("/", 1)[0])
        with self.client.write(filepath, encoding="utf-8", overwrite=True) as writer:
            for record in data:
                writer.write(json.dumps(record, ensure_ascii=False) + "\n")
