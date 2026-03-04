"""
Checkpoint Manager — tracks stage completion per view for resumability.
"""

import logging

from migration_engine.config import MAX_RETRIES
from migration_engine.persistence.repos import ExecutionLogRepo

logger = logging.getLogger(__name__)


class CheckpointManager:
    """Manages checkpoint state for migration stages."""

    def __init__(self, run_id: str):
        self.run_id = run_id
        self._log = ExecutionLogRepo()

    def is_completed(self, view_name: str, stage: str) -> bool:
        """Check if a stage is already completed."""
        return self._log.is_completed(view_name, stage, self.run_id)

    def mark_done(self, view_name: str, stage: str, payload: dict = None) -> None:
        """Mark a stage as completed."""
        self._log.log(view_name, stage, "DONE", self.run_id, payload=payload)

    def mark_failed(self, view_name: str, stage: str, error: str) -> None:
        """Mark a stage as failed."""
        self._log.log(view_name, stage, "FAILED", self.run_id, error_message=error)

    def is_dead_letter(self, view_name: str, stage: str) -> bool:
        """Check if a view has exceeded max retries at a stage."""
        count = self._log.get_failure_count(view_name, stage, self.run_id)
        return count >= MAX_RETRIES
