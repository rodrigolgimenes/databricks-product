"""
Parallel Executor — thread pool for parallel view processing with batch control.
"""

import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Callable, List, Optional

from migration_engine.config import BATCH_SIZE, BATCH_PAUSE_SECONDS
from migration_engine.connectors.databricks_connector import DatabricksConnector

logger = logging.getLogger(__name__)


class ParallelExecutor:
    """Executes a function across items with controlled parallelism and batching."""

    def __init__(self, max_workers: int):
        self.max_workers = max_workers

    def run(
        self,
        items: List[Any],
        function: Callable[[Any], Optional[str]],
        batch_size: int = BATCH_SIZE,
        pause_seconds: float = BATCH_PAUSE_SECONDS,
    ) -> dict:
        """
        Execute function over items in batches with parallelism.

        Args:
            items: List of items to process
            function: Callable that takes one item and returns error string or None
            batch_size: Items per batch
            pause_seconds: Pause between batches

        Returns:
            Dict with success_count, failure_count, errors
        """
        total = len(items)
        success_count = 0
        failure_count = 0
        errors: List[str] = []

        for batch_start in range(0, total, batch_size):
            batch = items[batch_start:batch_start + batch_size]
            batch_num = (batch_start // batch_size) + 1
            total_batches = (total + batch_size - 1) // batch_size

            logger.info(
                "Batch %d/%d: processing %d items (%.1f%% complete)",
                batch_num, total_batches, len(batch),
                (batch_start / total) * 100 if total else 100,
            )

            with ThreadPoolExecutor(max_workers=self.max_workers) as pool:
                futures = {pool.submit(function, item): item for item in batch}

                for future in as_completed(futures):
                    item = futures[future]
                    try:
                        result = future.result()
                        if result is None:
                            success_count += 1
                        else:
                            failure_count += 1
                            errors.append(str(result))
                    except Exception as e:
                        failure_count += 1
                        errors.append(f"Unexpected error on {item}: {e}")
                        logger.error("Error processing %s: %s", item, e)

            # Close thread-local connections after batch
            DatabricksConnector.close_thread_connection()

            # Pause between batches to avoid throttling
            if batch_start + batch_size < total and pause_seconds > 0:
                logger.debug("Pausing %.1fs between batches", pause_seconds)
                time.sleep(pause_seconds)

        logger.info(
            "Completed: %d success, %d failures out of %d total",
            success_count, failure_count, total,
        )

        return {
            "success_count": success_count,
            "failure_count": failure_count,
            "errors": errors,
        }
