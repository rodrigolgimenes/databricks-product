/**
 * Jobs Routes - Scheduled Jobs Management Endpoints
 * 
 * This module provides REST API endpoints for creating, managing, and monitoring
 * scheduled jobs that run in Databricks.
 */

const { v4: uuidv4 } = require('uuid');
const { CronExpressionParser } = require('cron-parser');
const jobsApi = require('./databricks/jobsApi');

module.exports = function(app, db, portalCfg, sqlStringLiteral, sqlQueryObjects, getRequestUser) {
  
  // Helper function to validate cron expression
  function validateCronExpression(cronExpr) {
    try {
      CronExpressionParser.parse(cronExpr);
      return { valid: true };
    } catch (e) {
      return { valid: false, error: e.message };
    }
  }

  // Helper function to calculate next run time
  function calculateNextRun(scheduleType, cronExpression, timezone) {
    try {
      if (scheduleType === 'CRON' && cronExpression) {
        const interval = CronExpressionParser.parse(cronExpression, {
          tz: timezone || 'America/Sao_Paulo'
        });
        return interval.next().toDate();
      }
      return null;
    } catch (e) {
      console.warn('[JOBS] Error calculating next run:', e.message);
      return null;
    }
  }

  // Helper: convert standard 5-field cron (min hour dom month dow) to Quartz 6-field (sec min hour dom month dow)
  function toQuartzCron(cronExpr) {
    if (!cronExpr) return '0 0 7 * * ?';
    const parts = String(cronExpr).trim().split(/\s+/);
    // Already Quartz format (6 or 7 fields)
    if (parts.length >= 6) return cronExpr;
    // Standard 5-field cron: prepend seconds=0, replace dow '*' with '?'
    if (parts.length === 5) {
      const [min, hour, dom, month, dow] = parts;
      // Normalize leading zeros (e.g. '00' -> '0', '07' -> '7') for numeric fields
      const normMin = /^\d+$/.test(min) ? String(Number(min)) : min;
      const normHour = /^\d+$/.test(hour) ? String(Number(hour)) : hour;
      // Quartz requires '?' in either dom or dow when the other is specified
      const quartzDow = dow === '*' ? '?' : dow;
      const quartzDom = quartzDow !== '?' && dom === '*' ? '?' : dom;
      return `0 ${normMin} ${normHour} ${quartzDom} ${month} ${quartzDow}`;
    }
    // Fallback
    return '0 0 7 * * ?';
  }

  // ==================== HELPER: Import missing Databricks runs into portal history ====================
  async function syncDatabricksRuns(jobId, databricksJobId) {
    if (!databricksJobId) return 0;
    try {
      // 1. List recent runs from Databricks
      const dbRuns = await jobsApi.listJobRuns(databricksJobId, 15);
      if (!dbRuns || dbRuns.length === 0) return 0;

      // 2. Get existing databricks_run_ids in our table
      const existingRows = await sqlQueryObjects(
        `SELECT databricks_run_id FROM ${portalCfg.ctrlSchema}.job_execution_history ` +
        `WHERE job_id = ${sqlStringLiteral(jobId)} AND databricks_run_id IS NOT NULL`
      );
      const existingRunIds = new Set(existingRows.map(r => String(r.databricks_run_id)));

      // 3. Find missing runs
      let imported = 0;
      let latestFinished = null; // track most recent finished run for last_run_at update

      for (const run of dbRuns) {
        const runId = run.run_id;
        if (existingRunIds.has(String(runId))) continue;

        // Map Databricks state to portal status
        const lifeCycle = run.state?.life_cycle_state;
        const resultState = run.state?.result_state;

        let status;
        if (lifeCycle === 'TERMINATED') {
          status = resultState === 'SUCCESS' ? 'SUCCEEDED'
                 : resultState === 'FAILED' ? 'FAILED'
                 : resultState === 'CANCELED' ? 'CANCELLED'
                 : resultState === 'TIMEDOUT' ? 'FAILED'
                 : 'COMPLETED';
        } else if (lifeCycle === 'INTERNAL_ERROR') {
          status = 'FAILED';
        } else if (lifeCycle === 'SKIPPED') {
          status = 'CANCELLED';
        } else if (lifeCycle === 'RUNNING' || lifeCycle === 'PENDING') {
          status = lifeCycle === 'RUNNING' ? 'RUNNING' : 'PENDING';
        } else {
          // QUEUED, BLOCKED, etc. — skip for now
          continue;
        }

        // Map trigger type
        const triggerType = run.trigger?.type;
        const triggeredBy = (triggerType === 'PERIODIC' || triggerType === 'CRON') ? 'SCHEDULE' : 'MANUAL';
        const triggeredByUser = triggeredBy === 'SCHEDULE' ? 'databricks-scheduler' : (run.creator_user_name || 'unknown');

        // Timestamps (Databricks uses epoch ms)
        const startMs = run.start_time || 0;
        const endMs = run.end_time || 0;
        const durationMs = run.execution_duration || (endMs > startMs ? endMs - startMs : 0);
        const startedAt = startMs ? new Date(startMs).toISOString().replace('T', ' ').replace('Z', '') : null;
        const finishedAt = endMs ? new Date(endMs).toISOString().replace('T', ' ').replace('Z', '') : null;

        // Run page URL
        const runPageUrl = run.run_page_url || `${process.env.DATABRICKS_HOST || ''}/#job/${databricksJobId}/run/${runId}`;

        // Error message
        const errorMsg = run.state?.state_message || null;

        // Insert into job_execution_history
        const executionId = require('crypto').randomUUID();
        await db.query(
          `INSERT INTO ${portalCfg.ctrlSchema}.job_execution_history (` +
          `  execution_id, job_id, databricks_run_id, started_at, finished_at, status, ` +
          `  duration_ms, triggered_by, triggered_by_user, run_page_url, error_message, created_at` +
          `) VALUES (` +
          `  ${sqlStringLiteral(executionId)}, ${sqlStringLiteral(jobId)}, ${runId}, ` +
          (startedAt ? `  TIMESTAMP ${sqlStringLiteral(startedAt)}, ` : '  NULL, ') +
          (finishedAt ? `  TIMESTAMP ${sqlStringLiteral(finishedAt)}, ` : '  NULL, ') +
          `  ${sqlStringLiteral(status)}, ${durationMs || 'NULL'}, ` +
          `  ${sqlStringLiteral(triggeredBy)}, ${sqlStringLiteral(triggeredByUser)}, ` +
          `  ${sqlStringLiteral(runPageUrl)}, ` +
          (errorMsg ? `  ${sqlStringLiteral(errorMsg.substring(0, 1000))}, ` : '  NULL, ') +
          (startedAt ? `  TIMESTAMP ${sqlStringLiteral(startedAt)}` : '  current_timestamp()') +
          `)`
        );

        imported++;
        console.log(`[JOBS-SYNC] Imported run ${runId} (${triggeredBy}, ${status}) for job ${jobId}`);

        // Track latest finished run
        if (finishedAt && (status === 'SUCCEEDED' || status === 'FAILED' || status === 'COMPLETED')) {
          if (!latestFinished || endMs > latestFinished.endMs) {
            latestFinished = { endMs, finishedAt, status, durationMs };
          }
        }
      }

      // 4. Update scheduled_jobs.last_run_at if we found a more recent run
      if (latestFinished) {
        await db.query(
          `UPDATE ${portalCfg.ctrlSchema}.scheduled_jobs ` +
          `SET last_run_at = TIMESTAMP ${sqlStringLiteral(latestFinished.finishedAt)}, ` +
          `    last_run_status = ${sqlStringLiteral(latestFinished.status)}, ` +
          `    last_run_duration_ms = ${latestFinished.durationMs || 'NULL'} ` +
          `WHERE job_id = ${sqlStringLiteral(jobId)} ` +
          `AND (last_run_at IS NULL OR last_run_at < TIMESTAMP ${sqlStringLiteral(latestFinished.finishedAt)})`
        );
      }

      if (imported > 0) {
        console.log(`[JOBS-SYNC] Imported ${imported} new run(s) from Databricks for job ${jobId}`);
      }
      return imported;
    } catch (err) {
      // Non-fatal: log and continue
      console.warn(`[JOBS-SYNC] Error syncing Databricks runs for job ${jobId}:`, err.message);
      return 0;
    }
  }

  // ==================== HELPER: Sync execution status from Databricks ====================
  async function syncRunningExecutions(executions) {
    const running = executions.filter(e => 
      (e.status === 'RUNNING' || e.status === 'PENDING') && e.databricks_run_id
    );
    if (running.length === 0) return executions;

    const updated = [...executions];
    for (const exec of running) {
      try {
        const runDetails = await jobsApi.getRunDetails(exec.databricks_run_id);
        if (!runDetails) continue;

        const lifeCycle = runDetails.state?.life_cycle_state;
        const resultState = runDetails.state?.result_state;

        // Only update if Databricks run is finished
        if (lifeCycle !== 'TERMINATED' && lifeCycle !== 'INTERNAL_ERROR' && lifeCycle !== 'SKIPPED') continue;

        let newStatus = resultState === 'SUCCESS' ? 'SUCCEEDED' : 
                          resultState === 'FAILED' ? 'FAILED' :
                          resultState === 'CANCELED' ? 'CANCELLED' :
                          resultState === 'TIMEDOUT' ? 'FAILED' : 'COMPLETED';

        const startMs = runDetails.start_time || 0;
        const endMs = runDetails.end_time || Date.now();
        const durationMs = endMs - startMs;
        const finishedAt = new Date(endMs).toISOString().replace('T', ' ').replace('Z', '');

        // Count datasets processed from run_queue
        // Use the portal execution's started_at (matches requested_at of enqueued datasets)
        // instead of Databricks start_time which is slightly later
        let datasetsProcessed = 0;
        let datasetsFailed = 0;
        let datasetsPending = 0;
        try {
          const execStartedAt = exec.started_at
            ? String(exec.started_at).replace('T', ' ').replace('Z', '').substring(0, 19)
            : new Date(startMs).toISOString().replace('T', ' ').replace('Z', '').substring(0, 19);
          const queueStats = await sqlQueryObjects(
            `SELECT status, COUNT(*) as cnt FROM ${portalCfg.opsSchema}.run_queue ` +
            `WHERE correlation_id = ${sqlStringLiteral(exec.job_id)} ` +
            `AND requested_at BETWEEN (TIMESTAMP ${sqlStringLiteral(execStartedAt)} - INTERVAL 30 SECONDS) ` +
            `AND (TIMESTAMP ${sqlStringLiteral(execStartedAt)} + INTERVAL 30 SECONDS) ` +
            `GROUP BY status`
          );
          for (const row of queueStats) {
            const st = String(row.status || '').toUpperCase();
            if (st === 'SUCCEEDED') datasetsProcessed += Number(row.cnt);
            if (st === 'FAILED') datasetsFailed += Number(row.cnt);
            if (st === 'PENDING') datasetsPending += Number(row.cnt);
          }
        } catch (qErr) {
          console.warn('[JOBS] Error counting queue stats:', qErr.message);
        }

        // Detect incomplete execution: Databricks finished but datasets still PENDING
        const datasetsTotal = Number(exec.datasets_total || 0);
        if (newStatus === 'SUCCEEDED' && datasetsTotal > 0 && datasetsPending > 0) {
          const processed = datasetsProcessed + datasetsFailed;
          console.warn(`[JOBS] ⚠️ Incomplete execution detected for ${exec.execution_id}: ` +
            `${processed}/${datasetsTotal} processed, ${datasetsPending} still PENDING`);
          newStatus = 'PARTIAL';
        }

        // Update execution history
        const errorMsg = runDetails.state?.state_message || null;
        const partialMsg = newStatus === 'PARTIAL'
          ? `Execução incompleta: ${datasetsProcessed + datasetsFailed}/${datasetsTotal} datasets processados, ${datasetsPending} ficaram na fila`
          : null;
        await db.query(
          `UPDATE ${portalCfg.ctrlSchema}.job_execution_history ` +
          `SET status = ${sqlStringLiteral(newStatus)}, ` +
          `    finished_at = TIMESTAMP ${sqlStringLiteral(finishedAt)}, ` +
          `    duration_ms = ${durationMs}, ` +
          `    datasets_processed = ${datasetsProcessed}, ` +
          `    datasets_failed = ${datasetsFailed}` +
          (errorMsg ? `, error_message = ${sqlStringLiteral(errorMsg.substring(0, 1000))}` : '') +
          (partialMsg ? `, error_message = ${sqlStringLiteral(partialMsg)}` : '') +
          ` WHERE execution_id = ${sqlStringLiteral(exec.execution_id)}`
        );

        // Update scheduled_jobs with last run info
        await db.query(
          `UPDATE ${portalCfg.ctrlSchema}.scheduled_jobs ` +
          `SET last_run_at = TIMESTAMP ${sqlStringLiteral(finishedAt)}, ` +
          `    last_run_status = ${sqlStringLiteral(newStatus)}, ` +
          `    last_run_duration_ms = ${durationMs} ` +
          `WHERE job_id = ${sqlStringLiteral(exec.job_id)}`
        );

        // Update in-memory array
        const idx = updated.findIndex(e => e.execution_id === exec.execution_id);
        if (idx >= 0) {
          updated[idx] = { ...updated[idx], status: newStatus, finished_at: finishedAt, duration_ms: durationMs, datasets_processed: datasetsProcessed, datasets_failed: datasetsFailed };
        }

        console.log(`[JOBS] Synced execution ${exec.execution_id}: ${exec.status} -> ${newStatus} (${durationMs}ms)`);
      } catch (err) {
        console.warn(`[JOBS] Error syncing execution ${exec.execution_id} via REST API:`, err.message);

        // ── FALLBACK: detect completion via run_queue when REST API fails ──
        try {
          const execStartedAt = exec.started_at
            ? String(exec.started_at).replace('T', ' ').replace('Z', '').substring(0, 19)
            : null;
          if (!execStartedAt) continue;

          const queueStats = await sqlQueryObjects(
            `SELECT status, COUNT(*) as cnt FROM ${portalCfg.opsSchema}.run_queue ` +
            `WHERE correlation_id = ${sqlStringLiteral(exec.job_id)} ` +
            `AND requested_at BETWEEN (TIMESTAMP ${sqlStringLiteral(execStartedAt)} - INTERVAL 30 SECONDS) ` +
            `AND (TIMESTAMP ${sqlStringLiteral(execStartedAt)} + INTERVAL 30 SECONDS) ` +
            `GROUP BY status`
          );

          let dsSucceeded = 0, dsFailed = 0, dsPending = 0, dsRunning = 0;
          for (const row of queueStats) {
            const st = String(row.status || '').toUpperCase();
            if (st === 'SUCCEEDED') dsSucceeded += Number(row.cnt);
            else if (st === 'FAILED') dsFailed += Number(row.cnt);
            else if (st === 'PENDING') dsPending += Number(row.cnt);
            else if (st === 'RUNNING' || st === 'CLAIMED') dsRunning += Number(row.cnt);
          }

          const dsTotal = dsSucceeded + dsFailed + dsPending + dsRunning;
          // If ALL datasets finished (none pending/running), mark execution as done
          if (dsTotal > 0 && dsPending === 0 && dsRunning === 0) {
            const newStatus = dsFailed > 0 ? 'FAILED' : 
                              dsSucceeded < Number(exec.datasets_total || dsTotal) ? 'PARTIAL' : 'SUCCEEDED';
            const now = new Date().toISOString().replace('T', ' ').replace('Z', '');
            const startMs = new Date(exec.started_at).getTime();
            const durationMs = Date.now() - startMs;

            await db.query(
              `UPDATE ${portalCfg.ctrlSchema}.job_execution_history ` +
              `SET status = ${sqlStringLiteral(newStatus)}, ` +
              `    finished_at = current_timestamp(), ` +
              `    duration_ms = ${durationMs}, ` +
              `    datasets_processed = ${dsSucceeded}, ` +
              `    datasets_failed = ${dsFailed} ` +
              `WHERE execution_id = ${sqlStringLiteral(exec.execution_id)}`
            );
            await db.query(
              `UPDATE ${portalCfg.ctrlSchema}.scheduled_jobs ` +
              `SET last_run_at = current_timestamp(), ` +
              `    last_run_status = ${sqlStringLiteral(newStatus)}, ` +
              `    last_run_duration_ms = ${durationMs} ` +
              `WHERE job_id = ${sqlStringLiteral(exec.job_id)}`
            );

            const idx = updated.findIndex(e => e.execution_id === exec.execution_id);
            if (idx >= 0) {
              updated[idx] = { ...updated[idx], status: newStatus, finished_at: now, duration_ms: durationMs, datasets_processed: dsSucceeded, datasets_failed: dsFailed };
            }
            console.log(`[JOBS] Synced execution ${exec.execution_id} via SQL fallback: ${exec.status} -> ${newStatus} (${dsSucceeded}ok/${dsFailed}fail)`);
          }
        } catch (fallbackErr) {
          console.warn(`[JOBS] SQL fallback also failed for ${exec.execution_id}:`, fallbackErr.message);
        }
      }
    }
    return updated;
  }

  // ==================== HELPER: Enrich dataset counts for completed executions ====================
  async function enrichDatasetCounts(executions) {
    // Enrich any completed execution missing dataset counts
    const needsEnrich = executions.filter(e => {
      const processed = Number(e.datasets_processed || 0);
      const failed = Number(e.datasets_failed || 0);
      return (processed + failed) === 0 &&
        !['RUNNING', 'PENDING'].includes(e.status) &&
        e.started_at && e.finished_at;
    });
    if (needsEnrich.length === 0) return executions;

    const updated = [...executions];
    for (const exec of needsEnrich) {
      try {
        const startedAt = String(exec.started_at).replace('T', ' ').replace('Z', '').substring(0, 19);
        const finishedAt = String(exec.finished_at).replace('T', ' ').replace('Z', '').substring(0, 19);

        // Count datasets that FINISHED during this execution's time window
        const queueStats = await sqlQueryObjects(
          `SELECT status, COUNT(*) as cnt FROM ${portalCfg.opsSchema}.run_queue ` +
          `WHERE correlation_id = ${sqlStringLiteral(exec.job_id)} ` +
          `AND finished_at BETWEEN TIMESTAMP ${sqlStringLiteral(startedAt)} ` +
          `AND (TIMESTAMP ${sqlStringLiteral(finishedAt)} + INTERVAL 1 MINUTE) ` +
          `GROUP BY status`
        );

        let processed = 0, failed = 0;
        for (const row of queueStats) {
          const st = String(row.status || '').toUpperCase();
          if (st === 'SUCCEEDED') processed += Number(row.cnt);
          if (st === 'FAILED') failed += Number(row.cnt);
        }

        // If datasets_total is missing, count enqueued items for this batch
        let total = Number(exec.datasets_total || 0);
        if (total === 0) {
          const totalResult = await sqlQueryObjects(
            `SELECT COUNT(*) as cnt FROM ${portalCfg.opsSchema}.run_queue ` +
            `WHERE correlation_id = ${sqlStringLiteral(exec.job_id)} ` +
            `AND requested_at BETWEEN (TIMESTAMP ${sqlStringLiteral(startedAt)} - INTERVAL 30 SECONDS) ` +
            `AND (TIMESTAMP ${sqlStringLiteral(startedAt)} + INTERVAL 30 SECONDS)`
          );
          total = Number(totalResult[0]?.cnt || 0);
        }
        // Fallback: if no enqueue batch found, total = what was actually processed
        if (total === 0 && (processed + failed) > 0) {
          total = processed + failed;
        }

        if (processed > 0 || failed > 0 || total > 0) {
          const setClauses = [
            `datasets_processed = ${processed}`,
            `datasets_failed = ${failed}`,
          ];
          if (total > 0 && !exec.datasets_total) {
            setClauses.push(`datasets_total = ${total}`);
          }
          await db.query(
            `UPDATE ${portalCfg.ctrlSchema}.job_execution_history ` +
            `SET ${setClauses.join(', ')} ` +
            `WHERE execution_id = ${sqlStringLiteral(exec.execution_id)}`
          );

          const idx = updated.findIndex(e => e.execution_id === exec.execution_id);
          if (idx >= 0) {
            updated[idx] = {
              ...updated[idx],
              datasets_processed: processed,
              datasets_failed: failed,
              datasets_total: total || updated[idx].datasets_total,
            };
          }
          console.log(`[JOBS] Enriched dataset counts for ${exec.execution_id}: ${processed} ok, ${failed} failed, total ${total}`);
        }
      } catch (err) {
        console.warn(`[JOBS] Error enriching dataset counts for ${exec.execution_id}:`, err.message);
      }
    }
    return updated;
  }

  // ==================== HELPER: Enrich RUNNING executions with real-time dataset counts ====================
  async function enrichRunningDatasetCounts(executions) {
    const running = executions.filter(e => ['RUNNING', 'PENDING'].includes(e.status));
    if (running.length === 0) return executions;

    const updated = [...executions];
    for (const exec of running) {
      try {
        const startedAt = String(exec.started_at || '').replace('T', ' ').replace('Z', '').substring(0, 19);
        if (!startedAt) continue;

        // Count current status of datasets in run_queue for this execution
        const queueStats = await sqlQueryObjects(
          `SELECT status, COUNT(*) as cnt FROM ${portalCfg.opsSchema}.run_queue ` +
          `WHERE correlation_id = ${sqlStringLiteral(exec.job_id)} ` +
          `AND requested_at BETWEEN (TIMESTAMP ${sqlStringLiteral(startedAt)} - INTERVAL 30 SECONDS) ` +
          `AND (TIMESTAMP ${sqlStringLiteral(startedAt)} + INTERVAL 30 SECONDS) ` +
          `GROUP BY status`
        );

        let processed = 0, failed = 0, pending = 0, running_ds = 0;
        for (const row of queueStats) {
          const st = String(row.status || '').toUpperCase();
          if (st === 'SUCCEEDED') processed += Number(row.cnt);
          if (st === 'FAILED') failed += Number(row.cnt);
          if (st === 'PENDING') pending += Number(row.cnt);
          if (st === 'RUNNING' || st === 'CLAIMED') running_ds += Number(row.cnt);
        }

        const total = Number(exec.datasets_total || 0) || (processed + failed + pending + running_ds);

        const idx = updated.findIndex(e => e.execution_id === exec.execution_id);
        if (idx >= 0) {
          updated[idx] = {
            ...updated[idx],
            datasets_processed: processed,
            datasets_failed: failed,
            datasets_total: total,
          };
        }
      } catch (err) {
        console.warn(`[JOBS] Error enriching running dataset counts for ${exec.execution_id}:`, err.message);
      }
    }
    return updated;
  }

  // ==================== CREATE JOB ====================
  app.post('/api/portal/jobs', async (req, res) => {
    const user = getRequestUser(req);
    console.log('[JOBS] POST /jobs - Creating new job, user:', user);

    try {
      const {
        job_name,
        description,
        schedule_type, // DAILY, WEEKLY, MONTHLY, CRON, ONCE
        cron_expression,
        timezone,
        project_id,
        area_id,
        max_concurrent_runs,
        retry_on_timeout,
        timeout_seconds,
        notification_email,
        dataset_ids // Array of dataset IDs to associate
      } = req.body;

      // Validation
      if (!job_name || !schedule_type || !project_id || !area_id) {
        return res.status(400).json({
          ok: false,
          error: 'MISSING_REQUIRED_FIELDS',
          message: 'job_name, schedule_type, project_id e area_id são obrigatórios'
        });
      }

      // Validate cron expression if provided
      if (schedule_type === 'CRON') {
        if (!cron_expression) {
          return res.status(400).json({
            ok: false,
            error: 'MISSING_CRON_EXPRESSION',
            message: 'cron_expression é obrigatório para schedule_type CRON'
          });
        }
        const cronValidation = validateCronExpression(cron_expression);
        if (!cronValidation.valid) {
          return res.status(400).json({
            ok: false,
            error: 'INVALID_CRON_EXPRESSION',
            message: `Cron expression inválida: ${cronValidation.error}`
          });
        }
      }

      // Check if job name already exists
      const existingJob = await sqlQueryObjects(
        `SELECT job_id FROM ${portalCfg.ctrlSchema}.scheduled_jobs ` +
        `WHERE job_name = ${sqlStringLiteral(job_name)} LIMIT 1`
      );
      if (existingJob.length > 0) {
        return res.status(409).json({
          ok: false,
          error: 'JOB_NAME_EXISTS',
          message: 'Já existe um job com este nome'
        });
      }

      // Generate job ID
      const jobId = uuidv4();
      const now = new Date().toISOString().replace('T', ' ').replace('Z', '');

      // Create job in Databricks first
      let databricksJobId = null;
      let databricksJobState = 'PENDING';
      
      try {
        const databricksResult = await jobsApi.createJob({
          job_name,
          job_id: jobId,
          cron_expression: toQuartzCron(cron_expression),
          timezone: timezone || 'America/Sao_Paulo',
          max_concurrent_runs: max_concurrent_runs || 1,
          timeout_seconds: timeout_seconds || 86400,
          notification_email
        });
        databricksJobId = databricksResult.databricks_job_id;
        databricksJobState = 'ACTIVE';
        console.log(`[JOBS] Databricks job created: ${databricksJobId}`);
      } catch (dbError) {
        console.error('[JOBS] Error creating Databricks job:', dbError.message);
        // Continue anyway - job can be synced later
      }

      // Calculate next run time
      const nextRunAt = calculateNextRun(schedule_type, cron_expression, timezone);

      // Insert into scheduled_jobs table
      const insertSql = `
        INSERT INTO ${portalCfg.ctrlSchema}.scheduled_jobs (
          job_id, job_name, description, schedule_type, cron_expression, timezone,
          enabled, databricks_job_id, databricks_job_state, project_id, area_id,
          max_concurrent_runs, retry_on_timeout, timeout_seconds, priority,
          created_at, created_by, next_run_at
        ) VALUES (
          ${sqlStringLiteral(jobId)},
          ${sqlStringLiteral(job_name)},
          ${description ? sqlStringLiteral(description) : 'NULL'},
          ${sqlStringLiteral(schedule_type)},
          ${cron_expression ? sqlStringLiteral(cron_expression) : 'NULL'},
          ${sqlStringLiteral(timezone || 'America/Sao_Paulo')},
          true,
          ${databricksJobId || 'NULL'},
          ${sqlStringLiteral(databricksJobState)},
          ${sqlStringLiteral(project_id)},
          ${sqlStringLiteral(area_id)},
          ${max_concurrent_runs || 1},
          ${retry_on_timeout !== false},
          ${timeout_seconds || 86400},
          100,
          TIMESTAMP ${sqlStringLiteral(now)},
          ${sqlStringLiteral(user)},
          ${nextRunAt ? `TIMESTAMP ${sqlStringLiteral(nextRunAt.toISOString().replace('T', ' ').replace('Z', ''))}` : 'NULL'}
        )
      `;

      await db.query(insertSql);
      console.log(`[JOBS] Job created in database: ${jobId}`);

      // Associate datasets if provided
      if (dataset_ids && Array.isArray(dataset_ids) && dataset_ids.length > 0) {
        for (let i = 0; i < dataset_ids.length; i++) {
          const datasetId = dataset_ids[i];
          const jobDatasetId = uuidv4();
          await db.query(
            `INSERT INTO ${portalCfg.ctrlSchema}.job_datasets (
              job_dataset_id, job_id, dataset_id, execution_order, enabled, created_at, created_by
            ) VALUES (
              ${sqlStringLiteral(jobDatasetId)},
              ${sqlStringLiteral(jobId)},
              ${sqlStringLiteral(datasetId)},
              ${i},
              true,
              TIMESTAMP ${sqlStringLiteral(now)},
              ${sqlStringLiteral(user)}
            )`
          );
        }
        console.log(`[JOBS] ${dataset_ids.length} datasets associated with job`);
      }

      return res.json({
        ok: true,
        job_id: jobId,
        databricks_job_id: databricksJobId,
        message: databricksJobId 
          ? 'Job criado com sucesso no portal e Databricks' 
          : 'Job criado no portal (será sincronizado com Databricks)'
      });

    } catch (error) {
      console.error('[JOBS] Error creating job:', error);
      return res.status(500).json({
        ok: false,
        error: 'INTERNAL_ERROR',
        message: error.message
      });
    }
  });

  // ==================== LIST JOBS ====================
  app.get('/api/portal/jobs', async (req, res) => {
    const user = getRequestUser(req);
    console.log('[JOBS] GET /jobs - Listing jobs, user:', user);

    try {
      const {
        project_id,
        area_id,
        enabled,
        page = 1,
        page_size = 25
      } = req.query;

      const filters = [];
      if (project_id) filters.push(`project_id = ${sqlStringLiteral(project_id)}`);
      if (area_id) filters.push(`area_id = ${sqlStringLiteral(area_id)}`);
      if (enabled !== undefined) filters.push(`enabled = ${enabled === 'true'}`);

      const whereClause = filters.length > 0 ? `WHERE ${filters.join(' AND ')}` : '';
      const offset = (parseInt(page) - 1) * parseInt(page_size);

      // Get total count
      const countResult = await sqlQueryObjects(
        `SELECT COUNT(*) as total FROM ${portalCfg.ctrlSchema}.scheduled_jobs ${whereClause}`
      );
      const total = countResult[0]?.total || 0;

      // Get jobs
      const jobs = await sqlQueryObjects(
        `SELECT * FROM ${portalCfg.ctrlSchema}.scheduled_jobs 
         ${whereClause}
         ORDER BY created_at DESC
         LIMIT ${parseInt(page_size)} OFFSET ${offset}`
      );

      // Get dataset counts and latest execution status for each job
      for (const job of jobs) {
        const datasetCount = await sqlQueryObjects(
          `SELECT COUNT(*) as count FROM ${portalCfg.ctrlSchema}.job_datasets 
           WHERE job_id = ${sqlStringLiteral(job.job_id)} AND enabled = true`
        );
        job.dataset_count = datasetCount[0]?.count || 0;

        // Latest execution status from history
        const latestExec = await sqlQueryObjects(
          `SELECT status, started_at, finished_at
           FROM ${portalCfg.ctrlSchema}.job_execution_history
           WHERE job_id = ${sqlStringLiteral(job.job_id)}
           ORDER BY started_at DESC LIMIT 1`
        );
        if (latestExec.length > 0) {
          job.latest_execution_status = latestExec[0].status;
        }
      }

      return res.json({
        ok: true,
        jobs,
        total,
        page: parseInt(page),
        page_size: parseInt(page_size)
      });

    } catch (error) {
      console.error('[JOBS] Error listing jobs:', error);
      return res.status(500).json({
        ok: false,
        error: 'INTERNAL_ERROR',
        message: error.message
      });
    }
  });

  // ==================== SYNC STATUS (CHECK DRIFT) ====================
  // NOTE: Must be registered BEFORE /jobs/:job_id to avoid :job_id capturing "sync-status"
  app.get('/api/portal/jobs/sync-status', async (req, res) => {
    console.log('[JOBS] GET /jobs/sync-status - Checking drift between portal and Databricks');

    try {
      const jobs = await sqlQueryObjects(
        `SELECT job_id, job_name, databricks_job_id, databricks_job_state, enabled
         FROM ${portalCfg.ctrlSchema}.scheduled_jobs
         WHERE databricks_job_id IS NOT NULL`
      );

      const driftReport = [];

      for (const job of jobs) {
        try {
          const dbJob = await jobsApi.getJobStatus(job.databricks_job_id);
          
          if (!dbJob) {
            driftReport.push({
              job_id: job.job_id,
              job_name: job.job_name,
              drift_type: 'DELETED_IN_DATABRICKS',
              message: 'Job foi deletado manualmente no Databricks'
            });
          } else {
            const dbState = dbJob.settings?.schedule?.pause_status;
            const portalEnabled = String(job.enabled).toLowerCase() === 'true';
            const expectedDbState = portalEnabled ? 'UNPAUSED' : 'PAUSED';
            
            if (dbState !== expectedDbState) {
              driftReport.push({
                job_id: job.job_id,
                job_name: job.job_name,
                drift_type: 'STATE_MISMATCH',
                message: `Portal: ${portalEnabled ? 'enabled' : 'disabled'}, Databricks: ${dbState}`,
                portal_state: portalEnabled,
                databricks_state: dbState
              });
            }
          }
        } catch (error) {
          console.warn(`[JOBS] Error checking drift for job ${job.job_id}:`, error.message);
        }
      }

      return res.json({
        ok: true,
        total_jobs: jobs.length,
        drifts_found: driftReport.length,
        drifts: driftReport
      });

    } catch (error) {
      console.error('[JOBS] Error checking sync status:', error);
      return res.status(500).json({
        ok: false,
        error: 'INTERNAL_ERROR',
        message: error.message
      });
    }
  });

  // ==================== GET JOB DETAILS ====================
  app.get('/api/portal/jobs/:job_id', async (req, res) => {
    const { job_id } = req.params;
    const user = getRequestUser(req);
    console.log('[JOBS] GET /jobs/:job_id - Getting job details, job_id:', job_id);

    try {
      // Get job
      const jobs = await sqlQueryObjects(
        `SELECT * FROM ${portalCfg.ctrlSchema}.scheduled_jobs 
         WHERE job_id = ${sqlStringLiteral(job_id)} LIMIT 1`
      );

      if (jobs.length === 0) {
        return res.status(404).json({
          ok: false,
          error: 'JOB_NOT_FOUND',
          message: 'Job não encontrado'
        });
      }

      const job = jobs[0];

      // Get associated datasets
      const datasets = await sqlQueryObjects(
        `SELECT jd.*, dc.dataset_name, dc.bronze_table, dc.silver_table, dc.incremental_strategy, dc.source_type, dc.execution_state
         FROM ${portalCfg.ctrlSchema}.job_datasets jd
         JOIN ${portalCfg.ctrlSchema}.dataset_control dc ON jd.dataset_id = dc.dataset_id
         WHERE jd.job_id = ${sqlStringLiteral(job_id)}
         ORDER BY jd.execution_order ASC`
      );

      // Add computed fields
      job.dataset_count = datasets.length;

      // Get recent executions
      let executions = await sqlQueryObjects(
        `SELECT * FROM ${portalCfg.ctrlSchema}.job_execution_history
         WHERE job_id = ${sqlStringLiteral(job_id)}
         ORDER BY started_at DESC
         LIMIT 10`
      );

      // Auto-sync: import any Databricks runs not yet in portal history
      if (job.databricks_job_id) {
        await syncDatabricksRuns(job_id, job.databricks_job_id);
        // Re-fetch executions after importing new runs
        executions = await sqlQueryObjects(
          `SELECT * FROM ${portalCfg.ctrlSchema}.job_execution_history
           WHERE job_id = ${sqlStringLiteral(job_id)}
           ORDER BY started_at DESC
           LIMIT 10`
        );
      }

      // Auto-sync: check Databricks for any RUNNING executions that may have finished
      executions = await syncRunningExecutions(executions);

      // Enrich RUNNING executions with real-time dataset progress from run_queue
      executions = await enrichRunningDatasetCounts(executions);

      // Re-fetch job after sync (last_run_* fields may have been updated)
      const jobsAfterSync = await sqlQueryObjects(
        `SELECT * FROM ${portalCfg.ctrlSchema}.scheduled_jobs 
         WHERE job_id = ${sqlStringLiteral(job_id)} LIMIT 1`
      );
      const jobFinal = jobsAfterSync[0] || job;
      jobFinal.dataset_count = datasets.length;
      if (datasets.length > 0) jobFinal.datasets = datasets;

      // Get active queue status for this job
      const queueItems = await sqlQueryObjects(
        `SELECT rq.queue_id, rq.dataset_id, rq.status, rq.trigger_type,
                rq.requested_at, rq.started_at, rq.finished_at,
                rq.last_error_class, rq.last_error_message,
                dc.dataset_name
         FROM ${portalCfg.opsSchema}.run_queue rq
         JOIN ${portalCfg.ctrlSchema}.dataset_control dc ON rq.dataset_id = dc.dataset_id
         WHERE rq.correlation_id = ${sqlStringLiteral(job_id)}
         AND rq.status IN ('PENDING', 'CLAIMED', 'RUNNING')
         ORDER BY rq.requested_at DESC`
      );

      return res.json({
        ok: true,
        job: jobFinal,
        datasets,
        recent_executions: executions,
        active_queue: queueItems
      });

    } catch (error) {
      console.error('[JOBS] Error getting job details:', error);
      return res.status(500).json({
        ok: false,
        error: 'INTERNAL_ERROR',
        message: error.message
      });
    }
  });

  // ==================== UPDATE JOB ====================
  app.patch('/api/portal/jobs/:job_id', async (req, res) => {
    const { job_id } = req.params;
    const user = getRequestUser(req);
    console.log('[JOBS] PATCH /jobs/:job_id - Updating job, job_id:', job_id);

    try {
      // Get existing job
      const jobs = await sqlQueryObjects(
        `SELECT * FROM ${portalCfg.ctrlSchema}.scheduled_jobs 
         WHERE job_id = ${sqlStringLiteral(job_id)} LIMIT 1`
      );

      if (jobs.length === 0) {
        return res.status(404).json({
          ok: false,
          error: 'JOB_NOT_FOUND',
          message: 'Job não encontrado'
        });
      }

      const job = jobs[0];
      const updates = [];
      const {
        job_name,
        description,
        cron_expression,
        timezone,
        max_concurrent_runs,
        timeout_seconds,
        notification_email,
        dataset_ids
      } = req.body;

      // Validate cron if updating
      if (cron_expression) {
        const cronValidation = validateCronExpression(cron_expression);
        if (!cronValidation.valid) {
          return res.status(400).json({
            ok: false,
            error: 'INVALID_CRON_EXPRESSION',
            message: `Cron expression inválida: ${cronValidation.error}`
          });
        }
        updates.push(`cron_expression = ${sqlStringLiteral(cron_expression)}`);
        
        // Recalculate next run
        const nextRunAt = calculateNextRun(job.schedule_type, cron_expression, timezone || job.timezone);
        if (nextRunAt) {
          updates.push(`next_run_at = TIMESTAMP ${sqlStringLiteral(nextRunAt.toISOString().replace('T', ' ').replace('Z', ''))}`);
        }
      }

      if (job_name) updates.push(`job_name = ${sqlStringLiteral(job_name)}`);
      if (description !== undefined) updates.push(`description = ${sqlStringLiteral(description)}`);
      if (timezone) updates.push(`timezone = ${sqlStringLiteral(timezone)}`);
      if (max_concurrent_runs !== undefined) updates.push(`max_concurrent_runs = ${max_concurrent_runs}`);
      if (timeout_seconds !== undefined) updates.push(`timeout_seconds = ${timeout_seconds}`);

      if (updates.length === 0 && !Array.isArray(dataset_ids)) {
        return res.status(400).json({
          ok: false,
          error: 'NO_UPDATES',
          message: 'Nenhuma atualização fornecida'
        });
      }

      const now = new Date().toISOString().replace('T', ' ').replace('Z', '');

      if (updates.length > 0) {
        updates.push(`updated_at = TIMESTAMP ${sqlStringLiteral(now)}`);
        updates.push(`updated_by = ${sqlStringLiteral(user)}`);

        // Update database
        await db.query(
          `UPDATE ${portalCfg.ctrlSchema}.scheduled_jobs 
           SET ${updates.join(', ')}
           WHERE job_id = ${sqlStringLiteral(job_id)}`
        );
      }

      // Update Databricks job if it exists
      if (job.databricks_job_id) {
        try {
          await jobsApi.updateJob(job.databricks_job_id, {
            job_name: job_name || job.job_name,
            cron_expression,
            timezone,
            max_concurrent_runs,
            timeout_seconds,
            notification_email
          });
          console.log(`[JOBS] Databricks job updated: ${job.databricks_job_id}`);
        } catch (dbError) {
          console.warn('[JOBS] Warning: Failed to update Databricks job:', dbError.message);
        }
      }

      // ── Sync dataset associations if dataset_ids provided ──
      if (Array.isArray(dataset_ids)) {
        // Get current datasets
        const currentDatasets = await sqlQueryObjects(
          `SELECT dataset_id FROM ${portalCfg.ctrlSchema}.job_datasets 
           WHERE job_id = ${sqlStringLiteral(job_id)}`
        );
        const currentIds = new Set(currentDatasets.map(d => d.dataset_id));
        const newIds = new Set(dataset_ids);

        // Remove datasets that were deselected
        for (const curr of currentIds) {
          if (!newIds.has(curr)) {
            await db.query(
              `DELETE FROM ${portalCfg.ctrlSchema}.job_datasets 
               WHERE job_id = ${sqlStringLiteral(job_id)} 
               AND dataset_id = ${sqlStringLiteral(curr)}`
            );
            console.log(`[JOBS] Removed dataset ${curr} from job ${job_id}`);
          }
        }

        // Add newly selected datasets
        const maxOrderResult = await sqlQueryObjects(
          `SELECT COALESCE(MAX(execution_order), -1) as max_order 
           FROM ${portalCfg.ctrlSchema}.job_datasets 
           WHERE job_id = ${sqlStringLiteral(job_id)}`
        );
        let nextOrder = (Number(maxOrderResult[0]?.max_order) || 0) + 1;
        const now2 = new Date().toISOString().replace('T', ' ').replace('Z', '');

        for (const dsId of dataset_ids) {
          if (!currentIds.has(dsId)) {
            const jobDatasetId = uuidv4();
            await db.query(
              `INSERT INTO ${portalCfg.ctrlSchema}.job_datasets (
                job_dataset_id, job_id, dataset_id, execution_order, enabled, created_at, created_by
              ) VALUES (
                ${sqlStringLiteral(jobDatasetId)},
                ${sqlStringLiteral(job_id)},
                ${sqlStringLiteral(dsId)},
                ${nextOrder},
                true,
                TIMESTAMP ${sqlStringLiteral(now2)},
                ${sqlStringLiteral(user)}
              )`
            );
            nextOrder++;
            console.log(`[JOBS] Added dataset ${dsId} to job ${job_id}`);
          }
        }

        console.log(`[JOBS] Dataset sync complete for job ${job_id}: ${dataset_ids.length} total, removed ${[...currentIds].filter(id => !newIds.has(id)).length}, added ${dataset_ids.filter(id => !currentIds.has(id)).length}`);
      }

      return res.json({
        ok: true,
        message: 'Job atualizado com sucesso'
      });

    } catch (error) {
      console.error('[JOBS] Error updating job:', error);
      return res.status(500).json({
        ok: false,
        error: 'INTERNAL_ERROR',
        message: error.message
      });
    }
  });

  // ==================== TOGGLE JOB (ENABLE/DISABLE) ====================
  app.post('/api/portal/jobs/:job_id/toggle', async (req, res) => {
    const { job_id } = req.params;
    const user = getRequestUser(req);
    console.log('[JOBS] POST /jobs/:job_id/toggle - Toggling job, job_id:', job_id);

    try {
      // Get current state
      const jobs = await sqlQueryObjects(
        `SELECT * FROM ${portalCfg.ctrlSchema}.scheduled_jobs 
         WHERE job_id = ${sqlStringLiteral(job_id)} LIMIT 1`
      );

      if (jobs.length === 0) {
        return res.status(404).json({
          ok: false,
          error: 'JOB_NOT_FOUND',
          message: 'Job não encontrado'
        });
      }

      const job = jobs[0];
      // Databricks SQL returns booleans as strings "true"/"false"
      const currentEnabled = String(job.enabled).toLowerCase() === 'true';
      const newState = !currentEnabled;
      const now = new Date().toISOString().replace('T', ' ').replace('Z', '');

      // Update database
      await db.query(
        `UPDATE ${portalCfg.ctrlSchema}.scheduled_jobs 
         SET enabled = ${newState}, 
             updated_at = TIMESTAMP ${sqlStringLiteral(now)},
             updated_by = ${sqlStringLiteral(user)}
         WHERE job_id = ${sqlStringLiteral(job_id)}`
      );

      // Toggle pause in Databricks
      if (job.databricks_job_id) {
        try {
          await jobsApi.toggleJobPause(job.databricks_job_id, !newState);
          await db.query(
            `UPDATE ${portalCfg.ctrlSchema}.scheduled_jobs 
             SET databricks_job_state = ${sqlStringLiteral(newState ? 'ACTIVE' : 'PAUSED')}
             WHERE job_id = ${sqlStringLiteral(job_id)}`
          );
          console.log(`[JOBS] Databricks job ${newState ? 'unpaused' : 'paused'}: ${job.databricks_job_id}`);
        } catch (dbError) {
          console.warn('[JOBS] Warning: Failed to toggle Databricks job:', dbError.message);
        }
      }

      return res.json({
        ok: true,
        enabled: newState,
        message: newState ? 'Job ativado' : 'Job desativado'
      });

    } catch (error) {
      console.error('[JOBS] Error toggling job:', error);
      return res.status(500).json({
        ok: false,
        error: 'INTERNAL_ERROR',
        message: error.message
      });
    }
  });

  // ==================== RUN JOB NOW ====================
  app.post('/api/portal/jobs/:job_id/run-now', async (req, res) => {
    const { job_id } = req.params;
    const user = getRequestUser(req);
    console.log('[JOBS] POST /jobs/:job_id/run-now - Running job manually, job_id:', job_id);

    try {
      // Get job
      const jobs = await sqlQueryObjects(
        `SELECT * FROM ${portalCfg.ctrlSchema}.scheduled_jobs 
         WHERE job_id = ${sqlStringLiteral(job_id)} LIMIT 1`
      );

      if (jobs.length === 0) {
        return res.status(404).json({
          ok: false,
          error: 'JOB_NOT_FOUND',
          message: 'Job não encontrado'
        });
      }

      const job = jobs[0];

      // Guard: check if job already has a RUNNING or PENDING execution
      const runningExecs = await sqlQueryObjects(
        `SELECT execution_id, status, started_at FROM ${portalCfg.ctrlSchema}.job_execution_history
         WHERE job_id = ${sqlStringLiteral(job_id)}
         AND status IN ('RUNNING', 'PENDING')
         ORDER BY started_at DESC LIMIT 1`
      );
      if (runningExecs.length > 0) {
        const active = runningExecs[0];
        console.log(`[JOBS] Blocked duplicate run-now for job ${job_id}: execution ${active.execution_id} is ${active.status}`);
        return res.status(409).json({
          ok: false,
          error: 'JOB_ALREADY_RUNNING',
          message: `Este job já possui uma execução em andamento (status: ${active.status}). Aguarde a conclusão antes de executar novamente.`,
          active_execution_id: active.execution_id
        });
      }

      // Auto-sync: se não tem databricks_job_id, tenta criar agora
      if (!job.databricks_job_id) {
        console.log('[JOBS] Job not synced with Databricks, attempting auto-sync...');
        try {
          const databricksResult = await jobsApi.createJob({
            job_name: job.job_name,
            job_id: job.job_id,
            cron_expression: toQuartzCron(job.cron_expression),
            timezone: job.timezone || 'America/Sao_Paulo',
            max_concurrent_runs: job.max_concurrent_runs || 1,
            timeout_seconds: job.timeout_seconds || 86400
          });
          job.databricks_job_id = databricksResult.databricks_job_id;
          const now2 = new Date().toISOString().replace('T', ' ').replace('Z', '');
          await db.query(
            `UPDATE ${portalCfg.ctrlSchema}.scheduled_jobs 
             SET databricks_job_id = ${job.databricks_job_id},
                 databricks_job_state = 'ACTIVE',
                 updated_at = TIMESTAMP ${sqlStringLiteral(now2)}
             WHERE job_id = ${sqlStringLiteral(job_id)}`
          );
          console.log(`[JOBS] Auto-sync successful, databricks_job_id: ${job.databricks_job_id}`);
        } catch (syncError) {
          console.error('[JOBS] Auto-sync failed:', syncError.message);
          return res.status(400).json({
            ok: false,
            error: 'SYNC_FAILED',
            message: `Não foi possível sincronizar o job com Databricks: ${syncError.message}`
          });
        }
      }

      // Ensure Databricks job has sufficient timeout (min 3h = 10800s)
      const portalTimeout = Number(job.timeout_seconds) || 10800;
      const effectiveTimeout = Math.max(portalTimeout, 10800); // at least 3h
      try {
        await jobsApi.updateJob(job.databricks_job_id, {
          timeout_seconds: effectiveTimeout
        });
        // Also update portal DB if it was below minimum
        if (portalTimeout < 10800) {
          await db.query(
            `UPDATE ${portalCfg.ctrlSchema}.scheduled_jobs ` +
            `SET timeout_seconds = ${effectiveTimeout} ` +
            `WHERE job_id = ${sqlStringLiteral(job_id)}`
          );
          console.log(`[JOBS] Updated timeout for job ${job_id}: ${portalTimeout}s -> ${effectiveTimeout}s`);
        }
      } catch (timeoutErr) {
        console.warn(`[JOBS] Warning: Failed to sync timeout for job ${job_id}:`, timeoutErr.message);
      }

      // Enqueue datasets first
      const datasets = await sqlQueryObjects(
        `SELECT dataset_id FROM ${portalCfg.ctrlSchema}.job_datasets 
         WHERE job_id = ${sqlStringLiteral(job_id)} AND enabled = true
         ORDER BY execution_order ASC`
      );

      const now = new Date().toISOString().replace('T', ' ').replace('Z', '');
      const executionId = uuidv4();

      for (const ds of datasets) {
        const queueId = uuidv4();
        await db.query(
          `INSERT INTO ${portalCfg.opsSchema}.run_queue (
            queue_id, dataset_id, trigger_type, status, priority, requested_by, requested_at, correlation_id, job_id
          ) VALUES (
            ${sqlStringLiteral(queueId)},
            ${sqlStringLiteral(ds.dataset_id)},
            'MANUAL',
            'PENDING',
            ${job.priority || 100},
            ${sqlStringLiteral(user)},
            TIMESTAMP ${sqlStringLiteral(now)},
            ${sqlStringLiteral(job_id)},
            ${sqlStringLiteral(job_id)}
          )`
        );
      }

      // Trigger Databricks job (pass max_items so notebook processes ALL enqueued datasets)
      const runResult = await jobsApi.runJobNow(job.databricks_job_id, { job_id, max_items: String(datasets.length) });

      // Record execution in history
      await db.query(
        `INSERT INTO ${portalCfg.ctrlSchema}.job_execution_history (
          execution_id, job_id, databricks_run_id, started_at, status, 
          datasets_total, triggered_by, triggered_by_user, run_page_url
        ) VALUES (
          ${sqlStringLiteral(executionId)},
          ${sqlStringLiteral(job_id)},
          ${runResult.run_id},
          TIMESTAMP ${sqlStringLiteral(now)},
          'RUNNING',
          ${datasets.length},
          'MANUAL',
          ${sqlStringLiteral(user)},
          ${sqlStringLiteral(runResult.run_page_url)}
        )`
      );

      return res.json({
        ok: true,
        execution_id: executionId,
        databricks_run_id: runResult.run_id,
        run_page_url: runResult.run_page_url,
        message: `Job iniciado manualmente. ${datasets.length} dataset(s) enfileirado(s).`
      });

    } catch (error) {
      console.error('[JOBS] Error running job:', error);
      return res.status(500).json({
        ok: false,
        error: 'INTERNAL_ERROR',
        message: error.message
      });
    }
  });

  // ==================== GET JOB RUNS (EXECUTION HISTORY) ====================
  app.get('/api/portal/jobs/:job_id/runs', async (req, res) => {
    const { job_id } = req.params;
    const { page = 1, page_size = 50 } = req.query;
    
    console.log('[JOBS] GET /jobs/:job_id/runs - Getting execution history, job_id:', job_id);

    try {
      const offset = (parseInt(page) - 1) * parseInt(page_size);

      // Get total count
      const countResult = await sqlQueryObjects(
        `SELECT COUNT(*) as total FROM ${portalCfg.ctrlSchema}.job_execution_history 
         WHERE job_id = ${sqlStringLiteral(job_id)}`
      );
      const total = countResult[0]?.total || 0;

      // Get executions
      let executions = await sqlQueryObjects(
        `SELECT * FROM ${portalCfg.ctrlSchema}.job_execution_history
         WHERE job_id = ${sqlStringLiteral(job_id)}
         ORDER BY started_at DESC
         LIMIT ${parseInt(page_size)} OFFSET ${offset}`
      );

      // Auto-sync: import any Databricks runs not yet in portal history
      const jobForSync = await sqlQueryObjects(
        `SELECT databricks_job_id FROM ${portalCfg.ctrlSchema}.scheduled_jobs
         WHERE job_id = ${sqlStringLiteral(job_id)} LIMIT 1`
      );
      if (jobForSync.length > 0 && jobForSync[0].databricks_job_id) {
        const imported = await syncDatabricksRuns(job_id, jobForSync[0].databricks_job_id);
        if (imported > 0) {
          // Re-fetch executions after importing new runs
          executions = await sqlQueryObjects(
            `SELECT * FROM ${portalCfg.ctrlSchema}.job_execution_history
             WHERE job_id = ${sqlStringLiteral(job_id)}
             ORDER BY started_at DESC
             LIMIT ${parseInt(page_size)} OFFSET ${offset}`
          );
        }
      }

      // Auto-sync: check Databricks for any RUNNING executions that may have finished
      executions = await syncRunningExecutions(executions);

      // Enrich dataset counts for completed executions missing them
      executions = await enrichDatasetCounts(executions);

      // Enrich RUNNING executions with real-time dataset progress from run_queue
      executions = await enrichRunningDatasetCounts(executions);

      return res.json({
        ok: true,
        runs: executions,
        executions,
        total,
        page: parseInt(page),
        page_size: parseInt(page_size)
      });

    } catch (error) {
      console.error('[JOBS] Error getting job runs:', error);
      return res.status(500).json({
        ok: false,
        error: 'INTERNAL_ERROR',
        message: error.message
      });
    }
  });

  // ==================== ADD DATASETS TO JOB ====================
  app.post('/api/portal/jobs/:job_id/datasets', async (req, res) => {
    const { job_id } = req.params;
    const { dataset_ids } = req.body;
    const user = getRequestUser(req);
    
    console.log('[JOBS] POST /jobs/:job_id/datasets - Adding datasets to job, job_id:', job_id);

    try {
      if (!dataset_ids || !Array.isArray(dataset_ids) || dataset_ids.length === 0) {
        return res.status(400).json({
          ok: false,
          error: 'MISSING_DATASET_IDS',
          message: 'dataset_ids deve ser um array não vazio'
        });
      }

      // Get current max execution_order
      const maxOrder = await sqlQueryObjects(
        `SELECT COALESCE(MAX(execution_order), -1) as max_order 
         FROM ${portalCfg.ctrlSchema}.job_datasets 
         WHERE job_id = ${sqlStringLiteral(job_id)}`
      );
      let nextOrder = (Number(maxOrder[0]?.max_order) || 0) + 1;

      const now = new Date().toISOString().replace('T', ' ').replace('Z', '');

      for (const dataset_id of dataset_ids) {
        // Check if already associated
        const existing = await sqlQueryObjects(
          `SELECT job_dataset_id FROM ${portalCfg.ctrlSchema}.job_datasets 
           WHERE job_id = ${sqlStringLiteral(job_id)} 
           AND dataset_id = ${sqlStringLiteral(dataset_id)} LIMIT 1`
        );

        if (existing.length === 0) {
          const jobDatasetId = uuidv4();
          await db.query(
            `INSERT INTO ${portalCfg.ctrlSchema}.job_datasets (
              job_dataset_id, job_id, dataset_id, execution_order, enabled, created_at, created_by
            ) VALUES (
              ${sqlStringLiteral(jobDatasetId)},
              ${sqlStringLiteral(job_id)},
              ${sqlStringLiteral(dataset_id)},
              ${nextOrder},
              true,
              TIMESTAMP ${sqlStringLiteral(now)},
              ${sqlStringLiteral(user)}
            )`
          );
          nextOrder++;
        }
      }

      return res.json({
        ok: true,
        message: `${dataset_ids.length} dataset(s) adicionado(s) ao job`
      });

    } catch (error) {
      console.error('[JOBS] Error adding datasets to job:', error);
      return res.status(500).json({
        ok: false,
        error: 'INTERNAL_ERROR',
        message: error.message
      });
    }
  });

  // ==================== REMOVE DATASET FROM JOB ====================
  app.delete('/api/portal/jobs/:job_id/datasets/:dataset_id', async (req, res) => {
    const { job_id, dataset_id } = req.params;
    const user = getRequestUser(req);
    
    console.log('[JOBS] DELETE /jobs/:job_id/datasets/:dataset_id - Removing dataset from job');

    try {
      await db.query(
        `DELETE FROM ${portalCfg.ctrlSchema}.job_datasets 
         WHERE job_id = ${sqlStringLiteral(job_id)} 
         AND dataset_id = ${sqlStringLiteral(dataset_id)}`
      );

      return res.json({
        ok: true,
        message: 'Dataset removido do job'
      });

    } catch (error) {
      console.error('[JOBS] Error removing dataset from job:', error);
      return res.status(500).json({
        ok: false,
        error: 'INTERNAL_ERROR',
        message: error.message
      });
    }
  });

  // ==================== SYNC JOB WITH DATABRICKS ====================
  app.post('/api/portal/jobs/:job_id/sync', async (req, res) => {
    const { job_id } = req.params;
    const user = getRequestUser(req);
    console.log('[JOBS] POST /jobs/:job_id/sync - Syncing job with Databricks, job_id:', job_id);

    try {
      const jobs = await sqlQueryObjects(
        `SELECT * FROM ${portalCfg.ctrlSchema}.scheduled_jobs 
         WHERE job_id = ${sqlStringLiteral(job_id)} LIMIT 1`
      );

      if (jobs.length === 0) {
        return res.status(404).json({ ok: false, error: 'JOB_NOT_FOUND', message: 'Job não encontrado' });
      }

      const job = jobs[0];

      if (job.databricks_job_id) {
        return res.json({ ok: true, message: 'Job já está sincronizado', databricks_job_id: job.databricks_job_id });
      }

      const databricksResult = await jobsApi.createJob({
        job_name: job.job_name,
        job_id: job.job_id,
        cron_expression: toQuartzCron(job.cron_expression),
        timezone: job.timezone || 'America/Sao_Paulo',
        max_concurrent_runs: job.max_concurrent_runs || 1,
        timeout_seconds: job.timeout_seconds || 86400
      });

      const now = new Date().toISOString().replace('T', ' ').replace('Z', '');
      await db.query(
        `UPDATE ${portalCfg.ctrlSchema}.scheduled_jobs 
         SET databricks_job_id = ${databricksResult.databricks_job_id},
             databricks_job_state = 'ACTIVE',
             updated_at = TIMESTAMP ${sqlStringLiteral(now)},
             updated_by = ${sqlStringLiteral(user)}
         WHERE job_id = ${sqlStringLiteral(job_id)}`
      );

      console.log(`[JOBS] Job synced successfully, databricks_job_id: ${databricksResult.databricks_job_id}`);

      return res.json({
        ok: true,
        databricks_job_id: databricksResult.databricks_job_id,
        message: 'Job sincronizado com Databricks com sucesso'
      });

    } catch (error) {
      console.error('[JOBS] Error syncing job:', error);
      return res.status(500).json({
        ok: false,
        error: 'SYNC_FAILED',
        message: `Falha ao sincronizar com Databricks: ${error.message}`
      });
    }
  });

  // ==================== ENQUEUE DATASETS FOR JOB ====================
  app.post('/api/portal/jobs/:job_id/enqueue-datasets', async (req, res) => {
    const { job_id } = req.params;
    const { triggered_by = 'SCHEDULE' } = req.body;
    const user = getRequestUser(req);
    
    console.log('[JOBS] POST /jobs/:job_id/enqueue-datasets - Enqueuing datasets for job, job_id:', job_id);

    try {
      // Get job
      const jobs = await sqlQueryObjects(
        `SELECT * FROM ${portalCfg.ctrlSchema}.scheduled_jobs 
         WHERE job_id = ${sqlStringLiteral(job_id)} LIMIT 1`
      );

      if (jobs.length === 0) {
        return res.status(404).json({
          ok: false,
          error: 'JOB_NOT_FOUND',
          message: 'Job não encontrado'
        });
      }

      const job = jobs[0];

      // Check if there's already a RUNNING execution for this job (idempotency)
      const runningExec = await sqlQueryObjects(
        `SELECT execution_id FROM ${portalCfg.ctrlSchema}.job_execution_history 
         WHERE job_id = ${sqlStringLiteral(job_id)} 
         AND status IN ('PENDING', 'RUNNING') 
         LIMIT 1`
      );

      if (runningExec.length > 0) {
        console.log('[JOBS] Job already has a running execution, skipping enqueue');
        return res.json({
          ok: true,
          message: 'Job já possui uma execução em andamento',
          execution_id: runningExec[0].execution_id,
          enqueued: 0
        });
      }

      // Get datasets for this job
      const datasets = await sqlQueryObjects(
        `SELECT dataset_id FROM ${portalCfg.ctrlSchema}.job_datasets 
         WHERE job_id = ${sqlStringLiteral(job_id)} AND enabled = true
         ORDER BY execution_order ASC`
      );

      if (datasets.length === 0) {
        return res.status(400).json(
{
          ok: false,
          error: 'NO_DATASETS',
          message: 'Job não possui datasets associados'
        });
      }

      const now = new Date().toISOString().replace('T', ' ').replace('Z', '');
      const executionId = uuidv4();

      // Enqueue all datasets
      for (const ds of datasets) {
        const queueId = uuidv4();
        await db.query(
          `INSERT INTO ${portalCfg.opsSchema}.run_queue (
            queue_id, dataset_id, trigger_type, status, priority, 
            requested_by, requested_at, attempt, max_retries, correlation_id, job_id
          ) VALUES (
            ${sqlStringLiteral(queueId)},
            ${sqlStringLiteral(ds.dataset_id)},
            ${sqlStringLiteral(triggered_by)},
            'PENDING',
            ${job.priority || 100},
            ${sqlStringLiteral(triggered_by === 'MANUAL' ? user : 'SCHEDULE')},
            TIMESTAMP ${sqlStringLiteral(now)},
            0,
            3,
            ${sqlStringLiteral(job_id)},
            ${sqlStringLiteral(job_id)}
          )`
        );
      }

      // Create execution record
      await db.query(
        `INSERT INTO ${portalCfg.ctrlSchema}.job_execution_history (
          execution_id, job_id, started_at, status, 
          datasets_total, triggered_by, triggered_by_user
        ) VALUES (
          ${sqlStringLiteral(executionId)},
          ${sqlStringLiteral(job_id)},
          TIMESTAMP ${sqlStringLiteral(now)},
          'PENDING',
          ${datasets.length},
          ${sqlStringLiteral(triggered_by)},
          ${sqlStringLiteral(user)}
        )`
      );

      console.log(`[JOBS] ${datasets.length} datasets enqueued for job ${job_id}`);

      return res.json({
        ok: true,
        execution_id: executionId,
        enqueued: datasets.length,
        message: `${datasets.length} dataset(s) enfileirado(s) para execução`
      });

    } catch (error) {
      console.error('[JOBS] Error enqueuing datasets:', error);
      return res.status(500).json({
        ok: false,
        error: 'INTERNAL_ERROR',
        message: error.message
      });
    }
  });

  // ==================== EXECUTION COMPLETE CALLBACK ====================
  app.post('/api/portal/jobs/:job_id/execution-complete', async (req, res) => {
    const { job_id } = req.params;
    const {
      databricks_run_id,
      status,
      duration_ms,
      datasets_processed,
      datasets_failed,
      error_message
    } = req.body;
    
    console.log('[JOBS] POST /jobs/:job_id/execution-complete - Recording execution completion, job_id:', job_id);

    try {
      const now = new Date().toISOString().replace('T', ' ').replace('Z', '');

      // Update job execution history
      await db.query(
        `UPDATE ${portalCfg.ctrlSchema}.job_execution_history 
         SET finished_at = TIMESTAMP ${sqlStringLiteral(now)},
             status = ${sqlStringLiteral(status)},
             duration_ms = ${duration_ms || 'NULL'},
             datasets_processed = ${datasets_processed || 0},
             datasets_failed = ${datasets_failed || 0},
             error_message = ${error_message ? sqlStringLiteral(error_message) : 'NULL'}
         WHERE job_id = ${sqlStringLiteral(job_id)}
         AND status IN ('PENDING', 'RUNNING')
         ORDER BY started_at DESC
         LIMIT 1`
      );

      // Update scheduled_jobs with last run info
      await db.query(
        `UPDATE ${portalCfg.ctrlSchema}.scheduled_jobs 
         SET last_run_at = TIMESTAMP ${sqlStringLiteral(now)},
             last_run_status = ${sqlStringLiteral(status)},
             last_run_duration_ms = ${duration_ms || 'NULL'}
         WHERE job_id = ${sqlStringLiteral(job_id)}`
      );

      console.log(`[JOBS] Execution complete recorded for job ${job_id}: ${status}`);

      return res.json({
        ok: true,
        message: 'Execução registrada com sucesso'
      });

    } catch (error) {
      console.error('[JOBS] Error recording execution complete:', error);
      return res.status(500).json({
        ok: false,
        error: 'INTERNAL_ERROR',
        message: error.message
      });
    }
  });

  // ==================== REPLAY PREVIEW ====================
  // Shows which datasets are eligible for partial replay from a specific execution
  app.get('/api/portal/jobs/:job_id/replay-preview/:execution_id', async (req, res) => {
    const { job_id, execution_id } = req.params;
    console.log('[JOBS] GET /jobs/:job_id/replay-preview/:execution_id - Preview replay for execution:', execution_id);

    try {
      // 1. Get the execution
      const execRows = await sqlQueryObjects(
        `SELECT * FROM ${portalCfg.ctrlSchema}.job_execution_history ` +
        `WHERE execution_id = ${sqlStringLiteral(execution_id)} ` +
        `AND job_id = ${sqlStringLiteral(job_id)} LIMIT 1`
      );
      if (execRows.length === 0) {
        return res.status(404).json({ ok: false, error: 'EXECUTION_NOT_FOUND', message: 'Execução não encontrada' });
      }
      const exec = execRows[0];

      // 2. Get all datasets associated with this job
      const jobDatasets = await sqlQueryObjects(
        `SELECT jd.dataset_id, dc.dataset_name, dc.bronze_table, dc.silver_table, dc.source_type ` +
        `FROM ${portalCfg.ctrlSchema}.job_datasets jd ` +
        `JOIN ${portalCfg.ctrlSchema}.dataset_control dc ON jd.dataset_id = dc.dataset_id ` +
        `WHERE jd.job_id = ${sqlStringLiteral(job_id)} AND jd.enabled = true ` +
        `ORDER BY jd.execution_order ASC`
      );

      // 3. Get run_queue status for datasets from this execution window
      const startedAt = String(exec.started_at || '').replace('T', ' ').replace('Z', '').substring(0, 19);
      let queueItems = [];
      if (startedAt) {
        queueItems = await sqlQueryObjects(
          `SELECT dataset_id, status, started_at, finished_at, last_error_class, last_error_message ` +
          `FROM ${portalCfg.opsSchema}.run_queue ` +
          `WHERE correlation_id = ${sqlStringLiteral(job_id)} ` +
          `AND requested_at BETWEEN (TIMESTAMP ${sqlStringLiteral(startedAt)} - INTERVAL 30 SECONDS) ` +
          `AND (TIMESTAMP ${sqlStringLiteral(startedAt)} + INTERVAL 30 SECONDS)`
        );
      }

      // 4. Also check if any dataset already SUCCEEDED today (from any execution)
      const todayStart = new Date().toISOString().substring(0, 10) + ' 00:00:00';
      const todaySucceeded = await sqlQueryObjects(
        `SELECT DISTINCT dataset_id FROM ${portalCfg.opsSchema}.run_queue ` +
        `WHERE correlation_id = ${sqlStringLiteral(job_id)} ` +
        `AND status = 'SUCCEEDED' ` +
        `AND finished_at >= TIMESTAMP ${sqlStringLiteral(todayStart)}`
      );
      const succeededTodaySet = new Set(todaySucceeded.map(r => r.dataset_id));

      // Build queue status map (from this specific execution)
      const queueMap = new Map();
      for (const qi of queueItems) {
        queueMap.set(qi.dataset_id, qi);
      }

      // 5. Classify each dataset
      const datasets = jobDatasets.map(ds => {
        const qi = queueMap.get(ds.dataset_id);
        const succeededToday = succeededTodaySet.has(ds.dataset_id);
        const queueStatus = qi ? String(qi.status || '').toUpperCase() : null;

        let replay_status; // SUCCEEDED, FAILED, PENDING, NOT_ENQUEUED
        if (queueStatus === 'SUCCEEDED') replay_status = 'SUCCEEDED';
        else if (queueStatus === 'FAILED') replay_status = 'FAILED';
        else if (queueStatus === 'PENDING' || queueStatus === 'CLAIMED' || queueStatus === 'RUNNING') replay_status = 'PENDING';
        else if (succeededToday) replay_status = 'SUCCEEDED';
        else replay_status = 'NOT_ENQUEUED';

        return {
          dataset_id: ds.dataset_id,
          dataset_name: ds.dataset_name,
          source_type: ds.source_type,
          replay_status,
          succeeded_today: succeededToday,
          error_class: qi?.last_error_class || null,
          error_message: qi?.last_error_message || null,
        };
      });

      const summary = {
        total: datasets.length,
        succeeded: datasets.filter(d => d.replay_status === 'SUCCEEDED').length,
        failed: datasets.filter(d => d.replay_status === 'FAILED').length,
        pending: datasets.filter(d => d.replay_status === 'PENDING').length,
        not_enqueued: datasets.filter(d => d.replay_status === 'NOT_ENQUEUED').length,
      };

      return res.json({
        ok: true,
        execution: {
          execution_id: exec.execution_id,
          status: exec.status,
          started_at: exec.started_at,
          finished_at: exec.finished_at,
          datasets_total: exec.datasets_total,
        },
        datasets,
        summary,
      });

    } catch (error) {
      console.error('[JOBS] Error building replay preview:', error);
      return res.status(500).json({ ok: false, error: 'INTERNAL_ERROR', message: error.message });
    }
  });

  // ==================== PARTIAL REPLAY ====================
  // Re-enqueue only selected/remaining datasets and trigger Databricks
  app.post('/api/portal/jobs/:job_id/replay', async (req, res) => {
    const { job_id } = req.params;
    const user = getRequestUser(req);
    const {
      execution_id,      // original execution being replayed
      mode,              // REMAINING_TODAY | FAILED_ONLY | ALL | SELECTED
      dataset_ids,       // only for mode=SELECTED
    } = req.body;

    console.log(`[JOBS] POST /jobs/:job_id/replay - mode=${mode}, execution_id=${execution_id}`);

    try {
      // 1. Get job
      const jobs = await sqlQueryObjects(
        `SELECT * FROM ${portalCfg.ctrlSchema}.scheduled_jobs ` +
        `WHERE job_id = ${sqlStringLiteral(job_id)} LIMIT 1`
      );
      if (jobs.length === 0) {
        return res.status(404).json({ ok: false, error: 'JOB_NOT_FOUND', message: 'Job não encontrado' });
      }
      const job = jobs[0];

      if (!job.databricks_job_id) {
        return res.status(400).json({ ok: false, error: 'NOT_SYNCED', message: 'Job não sincronizado com Databricks' });
      }

      // 2. Build the eligible datasets list based on mode
      // First get all job datasets
      const allJobDatasets = await sqlQueryObjects(
        `SELECT jd.dataset_id FROM ${portalCfg.ctrlSchema}.job_datasets jd ` +
        `WHERE jd.job_id = ${sqlStringLiteral(job_id)} AND jd.enabled = true ` +
        `ORDER BY jd.execution_order ASC`
      );

      // Get today's succeeded datasets for this job
      const todayStart = new Date().toISOString().substring(0, 10) + ' 00:00:00';
      const todaySucceeded = await sqlQueryObjects(
        `SELECT DISTINCT dataset_id FROM ${portalCfg.opsSchema}.run_queue ` +
        `WHERE correlation_id = ${sqlStringLiteral(job_id)} ` +
        `AND status = 'SUCCEEDED' ` +
        `AND finished_at >= TIMESTAMP ${sqlStringLiteral(todayStart)}`
      );
      const succeededTodaySet = new Set(todaySucceeded.map(r => r.dataset_id));

      // Get today's failed datasets
      const todayFailed = await sqlQueryObjects(
        `SELECT DISTINCT dataset_id FROM ${portalCfg.opsSchema}.run_queue ` +
        `WHERE correlation_id = ${sqlStringLiteral(job_id)} ` +
        `AND status = 'FAILED' ` +
        `AND finished_at >= TIMESTAMP ${sqlStringLiteral(todayStart)}`
      );
      const failedTodaySet = new Set(todayFailed.map(r => r.dataset_id));

      let datasetsToReplay = [];

      switch (mode) {
        case 'REMAINING_TODAY':
          // All datasets NOT yet succeeded today
          datasetsToReplay = allJobDatasets.filter(d => !succeededTodaySet.has(d.dataset_id));
          break;
        case 'FAILED_ONLY':
          // Only datasets that failed today
          datasetsToReplay = allJobDatasets.filter(d => failedTodaySet.has(d.dataset_id));
          break;
        case 'SELECTED':
          // User-selected datasets
          if (!dataset_ids || !Array.isArray(dataset_ids) || dataset_ids.length === 0) {
            return res.status(400).json({ ok: false, error: 'MISSING_DATASET_IDS', message: 'dataset_ids obrigatório para modo SELECTED' });
          }
          const selectedSet = new Set(dataset_ids);
          datasetsToReplay = allJobDatasets.filter(d => selectedSet.has(d.dataset_id));
          break;
        case 'ALL':
        default:
          datasetsToReplay = allJobDatasets;
          break;
      }

      if (datasetsToReplay.length === 0) {
        return res.status(400).json({ ok: false, error: 'NO_DATASETS', message: 'Nenhum dataset elegível para replay neste modo' });
      }

      // 3. Cancel any stale PENDING/CLAIMED items from previous executions for this job
      try {
        const cancelResult = await db.query(
          `UPDATE ${portalCfg.opsSchema}.run_queue ` +
          `SET status = 'CANCELLED' ` +
          `WHERE correlation_id = ${sqlStringLiteral(job_id)} ` +
          `AND status IN ('PENDING', 'CLAIMED') ` +
          `AND CAST(requested_at AS DATE) = CURRENT_DATE()`
        );
        console.log(`[JOBS] Cancelled stale PENDING items for job ${job_id} before replay`);
      } catch (cancelErr) {
        console.warn(`[JOBS] Warning: Could not cancel stale items:`, cancelErr.message);
      }

      // 4. Enqueue datasets
      const now = new Date().toISOString().replace('T', ' ').replace('Z', '');
      const newExecutionId = uuidv4();

      for (const ds of datasetsToReplay) {
        const queueId = uuidv4();
        await db.query(
          `INSERT INTO ${portalCfg.opsSchema}.run_queue (` +
          `  queue_id, dataset_id, trigger_type, status, priority, ` +
          `  requested_by, requested_at, correlation_id, job_id` +
          `) VALUES (` +
          `  ${sqlStringLiteral(queueId)},` +
          `  ${sqlStringLiteral(ds.dataset_id)},` +
          `  'MANUAL',` +
          `  'PENDING',` +
          `  ${job.priority || 100},` +
          `  ${sqlStringLiteral(user)},` +
          `  TIMESTAMP ${sqlStringLiteral(now)},` +
          `  ${sqlStringLiteral(job_id)},` +
          `  ${sqlStringLiteral(job_id)}` +
          `)`
        );
      }

      // 4. Trigger Databricks
      const runResult = await jobsApi.runJobNow(job.databricks_job_id, { job_id, max_items: String(datasetsToReplay.length) });

      // 5. Record execution in history
      const replayLabel = mode === 'ALL' ? 'Re-execução completa' : 
                          mode === 'REMAINING_TODAY' ? 'Retomada parcial (pendentes)' :
                          mode === 'FAILED_ONLY' ? 'Retry (somente falhos)' :
                          'Replay seletivo';
      await db.query(
        `INSERT INTO ${portalCfg.ctrlSchema}.job_execution_history (` +
        `  execution_id, job_id, databricks_run_id, started_at, status, ` +
        `  datasets_total, triggered_by, triggered_by_user, run_page_url, error_message` +
        `) VALUES (` +
        `  ${sqlStringLiteral(newExecutionId)},` +
        `  ${sqlStringLiteral(job_id)},` +
        `  ${runResult.run_id},` +
        `  TIMESTAMP ${sqlStringLiteral(now)},` +
        `  'RUNNING',` +
        `  ${datasetsToReplay.length},` +
        `  'MANUAL',` +
        `  ${sqlStringLiteral(user)},` +
        `  ${sqlStringLiteral(runResult.run_page_url)},` +
        `  ${sqlStringLiteral(replayLabel + (execution_id ? ` (ref: ${execution_id.substring(0, 8)})` : ''))}` +
        `)`
      );

      console.log(`[JOBS] Partial replay started: mode=${mode}, ${datasetsToReplay.length} datasets, execution=${newExecutionId}`);

      return res.json({
        ok: true,
        execution_id: newExecutionId,
        databricks_run_id: runResult.run_id,
        run_page_url: runResult.run_page_url,
        datasets_enqueued: datasetsToReplay.length,
        mode,
        message: `${replayLabel}: ${datasetsToReplay.length} dataset(s) enfileirado(s).`
      });

    } catch (error) {
      console.error('[JOBS] Error executing partial replay:', error);
      return res.status(500).json({ ok: false, error: 'INTERNAL_ERROR', message: error.message });
    }
  });

  // ==================== DELETE JOB ====================
  app.delete('/api/portal/jobs/:job_id', async (req, res) => {
    const { job_id } = req.params;
    const user = getRequestUser(req);
    console.log('[JOBS] DELETE /jobs/:job_id - Deleting job, job_id:', job_id);

    try {
      // Get job info
      const jobs = await sqlQueryObjects(
        `SELECT * FROM ${portalCfg.ctrlSchema}.scheduled_jobs 
         WHERE job_id = ${sqlStringLiteral(job_id)} LIMIT 1`
      );

      if (jobs.length === 0) {
        return res.status(404).json({ ok: false, error: 'JOB_NOT_FOUND', message: 'Job não encontrado' });
      }

      const job = jobs[0];

      // Delete from Databricks if synced
      if (job.databricks_job_id) {
        try {
          await jobsApi.deleteJob(job.databricks_job_id);
          console.log(`[JOBS] Deleted Databricks job ${job.databricks_job_id}`);
        } catch (dbError) {
          console.warn(`[JOBS] Warning: Could not delete Databricks job ${job.databricks_job_id}:`, dbError.message);
          // Continue with portal deletion even if Databricks deletion fails
        }
      }

      // Delete job_datasets associations
      await db.query(
        `DELETE FROM ${portalCfg.ctrlSchema}.job_datasets 
         WHERE job_id = ${sqlStringLiteral(job_id)}`
      );

      // Delete execution history
      await db.query(
        `DELETE FROM ${portalCfg.ctrlSchema}.job_execution_history 
         WHERE job_id = ${sqlStringLiteral(job_id)}`
      );

      // Delete the job itself
      await db.query(
        `DELETE FROM ${portalCfg.ctrlSchema}.scheduled_jobs 
         WHERE job_id = ${sqlStringLiteral(job_id)}`
      );

      console.log(`[JOBS] Job ${job_id} (${job.job_name}) deleted by ${user}`);

      return res.json({
        ok: true,
        message: `Job '${job.job_name}' excluído com sucesso`
      });

    } catch (error) {
      console.error('[JOBS] Error deleting job:', error);
      return res.status(500).json({
        ok: false,
        error: 'INTERNAL_ERROR',
        message: error.message
      });
    }
  });

  console.log('[JOBS] Jobs routes initialized');
};
