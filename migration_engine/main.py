"""
Migration Engine — Entry Point

Usage:
    python -m migration_engine.main [options]

Options:
    --phase PHASE     Run a specific phase only:
                      inventory, analysis, dependency, migration, finalize
    --batch-size N    Override batch size (default: 50)
    --workers N       Override max parallel workers for analysis (default: 16)
    --run-id ID       Resume a specific run
    --dry-run         Run analysis only (phases 0-2, no migration)
"""

import argparse
import logging
import sys

from migration_engine.config import BATCH_SIZE, MAX_WORKERS_ANALYSIS
from migration_engine.orchestrator.runner import MigrationRunner


def setup_logging(verbose: bool = False) -> None:
    """Configure logging."""
    level = logging.DEBUG if verbose else logging.INFO
    fmt = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    logging.basicConfig(level=level, format=fmt, stream=sys.stdout)

    # Quiet noisy libraries
    logging.getLogger("databricks").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)


def main():
    parser = argparse.ArgumentParser(
        description="Oracle Views → Databricks Migration Engine"
    )
    parser.add_argument(
        "--phase",
        choices=["inventory", "analysis", "dependency", "migration", "finalize"],
        default=None,
        help="Run a specific phase only (default: run all)",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=BATCH_SIZE,
        help=f"Batch size for processing (default: {BATCH_SIZE})",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=MAX_WORKERS_ANALYSIS,
        help=f"Max parallel workers for analysis (default: {MAX_WORKERS_ANALYSIS})",
    )
    parser.add_argument(
        "--run-id",
        default=None,
        help="Resume a specific run ID",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Run analysis only (phases 0-2), no migration",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Limit the number of views to migrate in phase 3 (0 = no limit)",
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Enable debug logging",
    )

    args = parser.parse_args()
    setup_logging(args.verbose)

    # Override config from CLI
    import migration_engine.config as cfg
    cfg.BATCH_SIZE = args.batch_size
    cfg.MAX_WORKERS_ANALYSIS = args.workers

    # Create runner
    runner = MigrationRunner(run_id=args.run_id, migration_limit=args.limit)

    if args.dry_run:
        # Run only analysis phases
        runner.execute(phase="inventory")
        runner.execute(phase="analysis")
        runner.execute(phase="dependency")
        logging.info("Dry run complete. No views were created.")
    elif args.phase:
        runner.execute(phase=args.phase)
    else:
        runner.execute()


if __name__ == "__main__":
    main()
