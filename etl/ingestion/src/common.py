import logging
from typing import Any, Dict, List

logger = logging.getLogger(__name__)


def add_column(list: List[Dict[str, Any]], column_name: str, value: Any) -> List[Dict[str, Any]]:
    return [{**item, column_name: value} for item in list if item is not None]
