"""
Migration Runner — orchestrates the full migration pipeline.

Phases:
  0. Inventory (extract views from xlsx)
  1. Analysis (parse, score, classify — parallel)
  2. Dependency Resolution (build graph, topological sort)
  3. Safe Migration (map, rewrite, validate, publish — sequential by dependency order)
  4. Finalization (consolidate results, generate report)
"""

import logging
import time
import uuid
from typing import Dict, List, Optional, Set

from migration_engine.config import (
    BATCH_SIZE,
    MAX_WORKERS_ANALYSIS,
    MAX_WORKERS_MIGRATION,
    TARGET_SCHEMA,
    Status,
)
from migration_engine.extractor.xlsx_extractor import ViewRecord, extract_views
from migration_engine.parser.sql_parser import parse_oracle_sql
from migration_engine.parser.ast_analyzer import StructuralMetrics, analyze
from migration_engine.scoring.classifier import Classification, classify
from migration_engine.mapping.table_mapper import TableMappingError, map_tables
from migration_engine.converter.sql_rewriter import rewrite_sql
from migration_engine.validator.column_validator import validate_columns
from migration_engine.validator.spark_validator import (
    ValidationError,
    validate_explain,
    validate_sample,
    create_temp_view,
    promote_view,
    drop_temp_view,
)
from migration_engine.dependency.dependency_graph import (
    build_dependency_graph,
    topological_sort,
)
from migration_engine.persistence.repos import (
    AnalysisRepo,
    ExecutionLogRepo,
    ResultRepo,
    create_control_tables,
)
from migration_engine.orchestrator.checkpoint_manager import CheckpointManager
from migration_engine.orchestrator.parallel_executor import ParallelExecutor

logger = logging.getLogger(__name__)


class MigrationRunner:
    """Main migration orchestrator."""

    def __init__(self, run_id: Optional[str] = None, migration_limit: int = 0):
        self.run_id = run_id or f"run_{uuid.uuid4().hex[:12]}"
        self.migration_limit = migration_limit  # 0 = no limit
        self.checkpoint = CheckpointManager(self.run_id)

        # In-memory state
        self.views: List[ViewRecord] = []
        self.metrics: Dict[str, StructuralMetrics] = {}
        self.classifications: Dict[str, Classification] = {}
        self.execution_order: List[str] = []
        self.cycle_views: Set[str] = set()

    def execute(self, phase: Optional[str] = None) -> Dict:
        """
        Execute the full migration pipeline (or a specific phase).

        Args:
            phase: Optional phase to run ("inventory", "analysis", "dependency",
                   "migration", "finalize"). If None, runs all phases.

        Returns:
            Summary dict with counts per status.
        """
        logger.info("=" * 60)
        logger.info("Migration Run: %s", self.run_id)
        logger.info("=" * 60)

        start_time = time.time()

        # Phase 0: Setup
        create_control_tables()
        self._create_target_schema()

        # Phase 0: Inventory
        if phase is None or phase == "inventory":
            self.phase_inventory()

        # Phase 1: Analysis
        if phase is None or phase == "analysis":
            self.phase_analysis()

        # Phase 2: Dependency Resolution
        if phase is None or phase == "dependency":
            self.phase_dependency_resolution()

        # Phase 3: Safe Migration
        if phase is None or phase == "migration":
            self.phase_safe_migration()

        # Phase 4: Finalization
        if phase is None or phase == "finalize":
            summary = self.phase_finalize()
        else:
            summary = {}

        elapsed = time.time() - start_time
        logger.info("=" * 60)
        logger.info("Migration completed in %.1fs", elapsed)
        logger.info("Summary: %s", summary)
        logger.info("=" * 60)

        return summary

    def _create_target_schema(self) -> None:
        """Ensure the target schema exists."""
        from migration_engine.connectors.databricks_connector import DatabricksConnector
        try:
            DatabricksConnector.execute_write(f"CREATE SCHEMA IF NOT EXISTS {TARGET_SCHEMA}")
            logger.info("Ensured target schema: %s", TARGET_SCHEMA)
        except Exception as e:
            logger.warning("Could not create target schema: %s", e)

    # ───────────────────────────────────────────────────────────────
    # PHASE 0: INVENTORY
    # ───────────────────────────────────────────────────────────────
    def phase_inventory(self) -> None:
        """Extract views from spreadsheet."""
        logger.info("Phase 0: INVENTORY")
        self.views = extract_views()
        logger.info("Inventory: %d views loaded", len(self.views))

    # ───────────────────────────────────────────────────────────────
    # PHASE 1: ANALYSIS (parallel)
    # ───────────────────────────────────────────────────────────────
    def phase_analysis(self) -> None:
        """Parse, score, and classify all views (parallel)."""
        logger.info("Phase 1: ANALYSIS (%d views)", len(self.views))

        executor = ParallelExecutor(max_workers=MAX_WORKERS_ANALYSIS)
        executor.run(self.views, self._analyze_single_view)

        # Reload any analysis that was skipped (already done from prior run)
        if not self.metrics:
            reloaded = self._reload_analysis_state()
            if reloaded:
                logger.info("Reloaded %d analysis records from prior run", reloaded)

        logger.info(
            "Analysis complete: %d metrics, %d classifications",
            len(self.metrics), len(self.classifications),
        )

    def _reload_analysis_state(self) -> int:
        """
        Reload analysis metrics and classifications from the DB for views
        already analyzed in this run. Enables resumability without re-running analysis.
        """
        rows = AnalysisRepo.get_all(self.run_id)
        loaded = 0
        for row in rows:
            vn = row["view_name"]
            if vn in self.metrics:
                continue  # already in memory

            # Reconstruct StructuralMetrics
            import json
            metrics = StructuralMetrics(
                table_count=row.get("table_count", 0),
                join_count=row.get("join_count", 0),
                subquery_count=row.get("subquery_count", 0),
                subquery_max_depth=row.get("subquery_max_depth", 0),
                group_by_count=row.get("group_by_count", 0),
                order_by_count=row.get("order_by_count", 0),
                case_count=row.get("case_count", 0),
                aggregate_count=row.get("aggregate_count", 0),
                analytic_function_count=row.get("analytic_function_count", 0),
                has_unmapped_function=bool(row.get("has_unmapped_function", False)),
                has_legacy_outer_join=bool(row.get("has_legacy_outer_join", False)),
                has_connect_by=bool(row.get("has_connect_by", False)),
                has_plsql_construct=bool(row.get("has_plsql_construct", False)),
                has_external_dependency=bool(row.get("has_external_dependency", False)),
                dependency_count=row.get("dependency_count", 0),
                tables=json.loads(row.get("tables_json", "[]")),
                functions_used=json.loads(row.get("functions_json", "[]")),
                raw_sql=row.get("original_sql", ""),
            )
            self.metrics[vn] = metrics

            # Reconstruct Classification
            cls = Classification(
                structural_score=row.get("structural_score", 0),
                risk_score=row.get("risk_score", 0),
                complexity_level=row.get("complexity_level", 0),
                risk_level=row.get("risk_level", ""),
                classification=row.get("classification", ""),
                eligible_for_auto_migration=bool(row.get("eligible_for_auto_migration", False)),
                ineligibility_reasons=json.loads(row.get("ineligibility_reasons_json", "[]")),
            )
            self.classifications[vn] = cls
            loaded += 1

        return loaded

    def _analyze_single_view(self, view: ViewRecord) -> Optional[str]:
        """Analyze a single view (called from parallel executor)."""
        view_name = view.view_name

        try:
            # Skip if already analyzed
            if self.checkpoint.is_completed(view_name, "CLASSIFY"):
                return None

            # Parse
            parse_result = parse_oracle_sql(view.original_sql)
            if not parse_result.success:
                self.checkpoint.mark_failed(view_name, "PARSE", parse_result.error or "Parse failed")
                # Save as PARSER_FAILED result
                ResultRepo.save(
                    view_name=view_name,
                    final_status=Status.PARSER_FAILED,
                    run_id=self.run_id,
                    error_message=parse_result.error or "Parse failed",
                )
                return None  # Not an executor error — it's handled

            self.checkpoint.mark_done(view_name, "PARSE")

            # Analyze AST — use sanitized SQL for raw_sql analysis
            effective_sql = parse_result.sanitized_sql or view.original_sql
            metrics = analyze(parse_result.expression, raw_sql=effective_sql)
            self.metrics[view_name] = metrics

            # Classify
            cls = classify(metrics)
            self.classifications[view_name] = cls

            # Persist
            AnalysisRepo.save(
                view_name=view_name,
                run_id=self.run_id,
                metrics=metrics,
                classification=cls,
                original_sql=view.original_sql,
                department=view.department or "",
                in_use=view.in_use or "",
            )

            self.checkpoint.mark_done(view_name, "CLASSIFY")
            return None

        except Exception as e:
            logger.error("Analysis failed for %s: %s", view_name, e)
            self.checkpoint.mark_failed(view_name, "CLASSIFY", str(e))
            return f"{view_name}: {e}"

    # ───────────────────────────────────────────────────────────────
    # PHASE 2: DEPENDENCY RESOLUTION
    # ───────────────────────────────────────────────────────────────
    def phase_dependency_resolution(self) -> None:
        """Build dependency graph and determine execution order."""
        logger.info("Phase 2: DEPENDENCY RESOLUTION")

        graph = build_dependency_graph(self.metrics)
        sorted_views, cycles = topological_sort(graph)

        self.execution_order = sorted_views
        self.cycle_views = set()
        for cycle_set in cycles:
            self.cycle_views.update(cycle_set)

        # Mark cyclic views as PENDING_REVIEW
        for view_name in self.cycle_views:
            ResultRepo.save(
                view_name=view_name,
                final_status=Status.PENDING_REVIEW,
                run_id=self.run_id,
                complexity_level=self.classifications.get(view_name, Classification(0, 0, 0, "", "", False, [])).complexity_level,
                risk_level=self.classifications.get(view_name, Classification(0, 0, 0, "", "", False, [])).risk_level,
                classification=self.classifications.get(view_name, Classification(0, 0, 0, "", "", False, [])).classification,
                error_message="CIRCULAR_DEPENDENCY",
            )

        logger.info(
            "Execution order: %d views (%d in cycles → PENDING_REVIEW)",
            len(self.execution_order), len(self.cycle_views),
        )

    # ───────────────────────────────────────────────────────────────
    # PHASE 3: SAFE MIGRATION
    # ───────────────────────────────────────────────────────────────
    def phase_safe_migration(self) -> None:
        """Migrate eligible views in dependency order."""
        logger.info("Phase 3: SAFE MIGRATION")

        # Filter to eligible views only
        eligible = [
            v for v in self.execution_order
            if v not in self.cycle_views
            and v in self.classifications
            and self.classifications[v].eligible_for_auto_migration
        ]

        non_eligible = [
            v for v in self.execution_order
            if v not in self.cycle_views
            and v in self.classifications
            and not self.classifications[v].eligible_for_auto_migration
        ]

        # Mark non-eligible as PENDING_REVIEW
        for view_name in non_eligible:
            if not self.checkpoint.is_completed(view_name, "FINALIZE"):
                cls = self.classifications[view_name]
                ResultRepo.save(
                    view_name=view_name,
                    final_status=Status.PENDING_REVIEW,
                    run_id=self.run_id,
                    complexity_level=cls.complexity_level,
                    risk_level=cls.risk_level,
                    classification=cls.classification,
                    error_message="; ".join(cls.ineligibility_reasons),
                )
                self.checkpoint.mark_done(view_name, "FINALIZE")

        # Apply limit if set
        if self.migration_limit > 0 and len(eligible) > self.migration_limit:
            logger.info(
                "Limiting migration to %d of %d eligible views",
                self.migration_limit, len(eligible),
            )
            eligible = eligible[:self.migration_limit]

        logger.info(
            "Migration: %d eligible (processing), %d non-eligible → PENDING_REVIEW",
            len(eligible), len(non_eligible),
        )

        # Process eligible views sequentially (in dependency order)
        # Use limited parallelism for publication
        executor = ParallelExecutor(max_workers=MAX_WORKERS_MIGRATION)
        executor.run(eligible, self._migrate_single_view)

    def _migrate_single_view(self, view_name: str) -> Optional[str]:
        """Migrate a single view through all stages."""
        try:
            # Skip if already finalized
            if self.checkpoint.is_completed(view_name, "FINALIZE"):
                return None

            # Check dead letter
            if self.checkpoint.is_dead_letter(view_name, "MAP_TABLES"):
                ResultRepo.save(
                    view_name=view_name,
                    final_status=Status.DEAD_LETTER,
                    run_id=self.run_id,
                    error_message="Dead letter: exceeded max retries",
                )
                return None

            metrics = self.metrics.get(view_name)
            cls = self.classifications.get(view_name)
            if not metrics or not cls:
                return f"{view_name}: No analysis data"

            # Find the original SQL
            original_sql = metrics.raw_sql

            # Stage: MAP_TABLES
            try:
                table_mapping = map_tables(metrics.tables)
                self.checkpoint.mark_done(view_name, "MAP_TABLES")
            except TableMappingError as e:
                self.checkpoint.mark_failed(view_name, "MAP_TABLES", str(e))
                ResultRepo.save(
                    view_name=view_name,
                    final_status=Status.PENDING_REVIEW,
                    run_id=self.run_id,
                    complexity_level=cls.complexity_level,
                    risk_level=cls.risk_level,
                    classification=cls.classification,
                    error_message=f"Table mapping failed: {e}",
                )
                return None

            # Stage: REWRITE_SQL — use sanitized SQL if available
            rewrite_result = rewrite_sql(original_sql, table_mapping, view_name)
            if not rewrite_result.success:
                self.checkpoint.mark_failed(view_name, "REWRITE_SQL", rewrite_result.error or "")
                ResultRepo.save(
                    view_name=view_name,
                    final_status=Status.FAILED_CONVERSION,
                    run_id=self.run_id,
                    complexity_level=cls.complexity_level,
                    risk_level=cls.risk_level,
                    classification=cls.classification,
                    error_message=rewrite_result.error or "Rewrite failed",
                )
                return None

            self.checkpoint.mark_done(view_name, "REWRITE_SQL")
            rewritten = rewrite_result.rewritten_sql

            # Stage: COLUMN_VALIDATION
            col_issues = validate_columns(rewritten, table_mapping)
            if col_issues:
                logger.warning("[%s] Column issues: %s", view_name, col_issues)
                # Non-blocking — continue but record warnings

            self.checkpoint.mark_done(view_name, "COLUMN_VALIDATION")

            # Stage: VALIDATE_EXPLAIN
            try:
                validate_explain(rewritten, view_name)
                self.checkpoint.mark_done(view_name, "VALIDATE_EXPLAIN")
            except ValidationError as e:
                self.checkpoint.mark_failed(view_name, "VALIDATE_EXPLAIN", str(e))
                ResultRepo.save(
                    view_name=view_name,
                    final_status=Status.FAILED_CONVERSION,
                    run_id=self.run_id,
                    complexity_level=cls.complexity_level,
                    risk_level=cls.risk_level,
                    classification=cls.classification,
                    rewritten_sql=rewritten,
                    error_message=str(e),
                )
                return None

            # Stage: VALIDATE_SAMPLE
            try:
                validate_sample(rewritten, view_name)
                self.checkpoint.mark_done(view_name, "VALIDATE_SAMPLE")
            except ValidationError as e:
                self.checkpoint.mark_failed(view_name, "VALIDATE_SAMPLE", str(e))
                ResultRepo.save(
                    view_name=view_name,
                    final_status=Status.FAILED_CONVERSION,
                    run_id=self.run_id,
                    complexity_level=cls.complexity_level,
                    risk_level=cls.risk_level,
                    classification=cls.classification,
                    rewritten_sql=rewritten,
                    error_message=str(e),
                )
                return None

            # Stage: TEMP_VIEW_CREATE
            try:
                create_temp_view(view_name, rewritten)
                self.checkpoint.mark_done(view_name, "TEMP_VIEW_CREATE")
            except ValidationError as e:
                self.checkpoint.mark_failed(view_name, "TEMP_VIEW_CREATE", str(e))
                ResultRepo.save(
                    view_name=view_name,
                    final_status=Status.FAILED_CONVERSION,
                    run_id=self.run_id,
                    rewritten_sql=rewritten,
                    error_message=str(e),
                )
                return None

            # Stage: PROMOTE_VIEW
            try:
                fqn = promote_view(view_name, rewritten)
                self.checkpoint.mark_done(view_name, "PROMOTE_VIEW")
            except ValidationError as e:
                self.checkpoint.mark_failed(view_name, "PROMOTE_VIEW", str(e))
                ResultRepo.save(
                    view_name=view_name,
                    final_status=Status.FAILED_CONVERSION,
                    run_id=self.run_id,
                    rewritten_sql=rewritten,
                    error_message=str(e),
                )
                return None
            finally:
                drop_temp_view(view_name)

            # Stage: FINALIZE
            warnings = rewrite_result.warnings + col_issues
            final_status = (
                Status.MIGRATED_WITH_WARNING if warnings
                else Status.MIGRATED_SUCCESS
            )

            ResultRepo.save(
                view_name=view_name,
                final_status=final_status,
                run_id=self.run_id,
                complexity_level=cls.complexity_level,
                risk_level=cls.risk_level,
                classification=cls.classification,
                rewritten_sql=rewritten,
                warnings=warnings,
            )

            self.checkpoint.mark_done(view_name, "FINALIZE")
            logger.info("[%s] Migrated successfully → %s", view_name, final_status)
            return None

        except Exception as e:
            logger.error("Unexpected error migrating %s: %s", view_name, e)
            ResultRepo.save(
                view_name=view_name,
                final_status=Status.FAILED_CONVERSION,
                run_id=self.run_id,
                error_message=f"Unexpected: {e}",
            )
            return f"{view_name}: {e}"

    # ───────────────────────────────────────────────────────────────
    # PHASE 4: FINALIZATION
    # ───────────────────────────────────────────────────────────────
    def phase_finalize(self) -> Dict:
        """Generate summary and consolidated report."""
        logger.info("Phase 4: FINALIZATION")

        summary = ResultRepo.get_summary(self.run_id)

        total = sum(summary.values())
        migrated = summary.get(Status.MIGRATED_SUCCESS, 0) + summary.get(Status.MIGRATED_WITH_WARNING, 0)
        pending = summary.get(Status.PENDING_REVIEW, 0)
        failed = summary.get(Status.FAILED_CONVERSION, 0)
        parser_failed = summary.get(Status.PARSER_FAILED, 0)
        dead_letter = summary.get(Status.DEAD_LETTER, 0)

        pct_migrated = (migrated / total * 100) if total else 0

        report = f"""
═══════════════════════════════════════════════════════
  MIGRATION REPORT — Run: {self.run_id}
═══════════════════════════════════════════════════════
  Total views:           {total}
  Migrated (success):    {summary.get(Status.MIGRATED_SUCCESS, 0)}
  Migrated (warning):    {summary.get(Status.MIGRATED_WITH_WARNING, 0)}
  Pending review:        {pending}
  Failed conversion:     {failed}
  Parser failed:         {parser_failed}
  Dead letter:           {dead_letter}
  ─────────────────────────────────────────────────────
  Auto-migration rate:   {pct_migrated:.1f}%
═══════════════════════════════════════════════════════
"""
        logger.info(report)
        print(report)

        return summary
