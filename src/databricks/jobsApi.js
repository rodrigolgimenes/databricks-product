/**
 * Databricks Jobs API Integration Module
 * 
 * This module provides functions to interact with the Databricks Jobs API (2.1)
 * for creating, managing, and monitoring scheduled jobs.
 * 
 * Documentation: https://docs.databricks.com/api/workspace/jobs
 */

const axios = require('axios');

// Databricks workspace configuration from environment
const DATABRICKS_HOST = process.env.DATABRICKS_HOST;
const DATABRICKS_TOKEN = process.env.DATABRICKS_TOKEN;
const DATABRICKS_CLUSTER_ID = process.env.DATABRICKS_CLUSTER_ID;
const PORTAL_URL = process.env.PORTAL_URL || 'http://localhost:3001';

if (!DATABRICKS_HOST || !DATABRICKS_TOKEN) {
  console.warn('⚠️  Databricks Jobs API credentials not configured');
}

/**
 * Create axios instance with Databricks authentication
 */
const databricksClient = axios.create({
  baseURL: `${DATABRICKS_HOST}/api/2.1`,
  headers: {
    'Authorization': `Bearer ${DATABRICKS_TOKEN}`,
    'Content-Type': 'application/json'
  },
  timeout: 30000
});

/**
 * Create a new Databricks Job
 * @param {Object} config - Job configuration
 * @param {string} config.job_name - Job name (will be prefixed with PORTAL_JOB_)
 * @param {string} config.job_id - Portal job UUID
 * @param {string} config.cron_expression - Quartz cron expression
 * @param {string} config.timezone - Timezone ID (e.g., America/Sao_Paulo)
 * @param {number} config.max_concurrent_runs - Max concurrent runs (default: 1)
 * @param {number} config.timeout_seconds - Job timeout in seconds
 * @param {string} config.notification_email - Email for failure notifications
 * @returns {Promise<{databricks_job_id: number}>}
 */
async function createJob(config) {
  const {
    job_name,
    job_id,
    cron_expression,
    timezone = 'America/Sao_Paulo',
    max_concurrent_runs = 1,
    timeout_seconds = 86400,
    notification_email
  } = config;

  const payload = {
    name: `PORTAL_JOB_${job_name}`,
    tasks: [{
      task_key: 'orchestrate_ingestion',
      notebook_task: {
        notebook_path: '/Shared/governed_ingestion_orchestrator',
        base_parameters: {
          job_id: job_id,
          portal_api_url: PORTAL_URL,
          max_items: '200'
        }
      },
      existing_cluster_id: DATABRICKS_CLUSTER_ID
    }],
    schedule: {
      quartz_cron_expression: cron_expression,
      timezone_id: timezone,
      pause_status: 'UNPAUSED'
    },
    max_concurrent_runs,
    timeout_seconds,
    tags: {
      managed_by: 'portal',
      job_id: job_id
    }
  };

  // Add email notifications if provided
  if (notification_email) {
    payload.email_notifications = {
      on_failure: [notification_email]
    };
  }

  try {
    const response = await databricksClient.post('/jobs/create', payload);
    return {
      databricks_job_id: response.data.job_id
    };
  } catch (error) {
    console.error('❌ Error creating Databricks job:', error.response?.data || error.message);
    throw new Error(`Failed to create Databricks job: ${error.response?.data?.message || error.message}`);
  }
}

/**
 * Update an existing Databricks Job
 * @param {number} databricksJobId - Databricks job ID
 * @param {Object} updates - Fields to update
 * @returns {Promise<void>}
 */
async function updateJob(databricksJobId, updates) {
  const {
    job_name,
    cron_expression,
    timezone,
    max_concurrent_runs,
    timeout_seconds,
    notification_email
  } = updates;

  // First get current job config
  const currentJob = await getJobStatus(databricksJobId);
  
  const payload = {
    job_id: databricksJobId,
    new_settings: {
      ...currentJob.settings,
      name: job_name ? `PORTAL_JOB_${job_name}` : currentJob.settings.name,
      max_concurrent_runs: max_concurrent_runs ?? currentJob.settings.max_concurrent_runs,
      timeout_seconds: timeout_seconds ?? currentJob.settings.timeout_seconds
    }
  };

  // Update schedule if provided
  if (cron_expression || timezone) {
    payload.new_settings.schedule = {
      quartz_cron_expression: cron_expression || currentJob.settings.schedule?.quartz_cron_expression,
      timezone_id: timezone || currentJob.settings.schedule?.timezone_id,
      pause_status: currentJob.settings.schedule?.pause_status || 'UNPAUSED'
    };
  }

  // Update email notifications if provided
  if (notification_email) {
    payload.new_settings.email_notifications = {
      on_failure: [notification_email]
    };
  }

  try {
    await databricksClient.post('/jobs/update', payload);
  } catch (error) {
    console.error('❌ Error updating Databricks job:', error.response?.data || error.message);
    throw new Error(`Failed to update Databricks job: ${error.response?.data?.message || error.message}`);
  }
}

/**
 * Delete a Databricks Job
 * @param {number} databricksJobId - Databricks job ID
 * @returns {Promise<void>}
 */
async function deleteJob(databricksJobId) {
  try {
    await databricksClient.post('/jobs/delete', {
      job_id: databricksJobId
    });
  } catch (error) {
    console.error('❌ Error deleting Databricks job:', error.response?.data || error.message);
    throw new Error(`Failed to delete Databricks job: ${error.response?.data?.message || error.message}`);
  }
}

/**
 * Pause or unpause a job
 * @param {number} databricksJobId - Databricks job ID
 * @param {boolean} pause - true to pause, false to unpause
 * @returns {Promise<void>}
 */
async function toggleJobPause(databricksJobId, pause) {
  const currentJob = await getJobStatus(databricksJobId);
  
  const payload = {
    job_id: databricksJobId,
    new_settings: {
      ...currentJob.settings,
      schedule: {
        ...currentJob.settings.schedule,
        pause_status: pause ? 'PAUSED' : 'UNPAUSED'
      }
    }
  };

  try {
    await databricksClient.post('/jobs/update', payload);
  } catch (error) {
    console.error('❌ Error toggling job pause:', error.response?.data || error.message);
    throw new Error(`Failed to toggle job pause: ${error.response?.data?.message || error.message}`);
  }
}

/**
 * Run a job now (manual trigger)
 * @param {number} databricksJobId - Databricks job ID
 * @param {Object} params - Optional parameters to pass to the job
 * @returns {Promise<{run_id: number, run_page_url: string}>}
 */
async function runJobNow(databricksJobId, params = {}) {
  try {
    const response = await databricksClient.post('/jobs/run-now', {
      job_id: databricksJobId,
      notebook_params: params
    });
    
    return {
      run_id: response.data.run_id,
      run_page_url: `${DATABRICKS_HOST}/#job/${databricksJobId}/run/${response.data.run_id}`
    };
  } catch (error) {
    console.error('❌ Error running job:', error.response?.data || error.message);
    throw new Error(`Failed to run job: ${error.response?.data?.message || error.message}`);
  }
}

/**
 * Get job status and configuration
 * @param {number} databricksJobId - Databricks job ID
 * @returns {Promise<Object>} Job details
 */
async function getJobStatus(databricksJobId) {
  try {
    const response = await databricksClient.get('/jobs/get', {
      params: { job_id: databricksJobId }
    });
    return response.data;
  } catch (error) {
    if (error.response?.status === 404) {
      return null; // Job doesn't exist
    }
    console.error('❌ Error getting job status:', error.response?.data || error.message);
    throw new Error(`Failed to get job status: ${error.response?.data?.message || error.message}`);
  }
}

/**
 * List recent runs for a job
 * @param {number} databricksJobId - Databricks job ID
 * @param {number} limit - Max number of runs to return (default: 25)
 * @returns {Promise<Array>} List of runs
 */
async function listJobRuns(databricksJobId, limit = 25) {
  try {
    const response = await databricksClient.get('/jobs/runs/list', {
      params: {
        job_id: databricksJobId,
        limit,
        expand_tasks: false
      }
    });
    return response.data.runs || [];
  } catch (error) {
    console.error('❌ Error listing job runs:', error.response?.data || error.message);
    throw new Error(`Failed to list job runs: ${error.response?.data?.message || error.message}`);
  }
}

/**
 * Get details of a specific run
 * @param {number} runId - Run ID
 * @returns {Promise<Object>} Run details
 */
async function getRunDetails(runId) {
  try {
    const response = await databricksClient.get('/jobs/runs/get', {
      params: { run_id: runId }
    });
    return response.data;
  } catch (error) {
    console.error('❌ Error getting run details:', error.response?.data || error.message);
    throw new Error(`Failed to get run details: ${error.response?.data?.message || error.message}`);
  }
}

/**
 * Cancel a running job
 * @param {number} runId - Run ID
 * @returns {Promise<void>}
 */
async function cancelRun(runId) {
  try {
    await databricksClient.post('/jobs/runs/cancel', {
      run_id: runId
    });
  } catch (error) {
    console.error('❌ Error canceling run:', error.response?.data || error.message);
    throw new Error(`Failed to cancel run: ${error.response?.data?.message || error.message}`);
  }
}

/**
 * Get output logs from a run
 * @param {number} runId - Run ID
 * @returns {Promise<{logs: string}>}
 */
async function getRunOutput(runId) {
  try {
    const response = await databricksClient.get('/jobs/runs/get-output', {
      params: { run_id: runId }
    });
    return {
      logs: response.data.logs || '',
      error: response.data.error,
      error_trace: response.data.error_trace
    };
  } catch (error) {
    console.error('❌ Error getting run output:', error.response?.data || error.message);
    throw new Error(`Failed to get run output: ${error.response?.data?.message || error.message}`);
  }
}

/**
 * List all portal-managed jobs (by tag)
 * @returns {Promise<Array>} List of jobs
 */
async function listPortalJobs() {
  try {
    const response = await databricksClient.get('/jobs/list', {
      params: {
        expand_tasks: false,
        limit: 100
      }
    });
    
    // Filter only portal-managed jobs
    const jobs = response.data.jobs || [];
    return jobs.filter(job => job.settings?.tags?.managed_by === 'portal');
  } catch (error) {
    console.error('❌ Error listing portal jobs:', error.response?.data || error.message);
    throw new Error(`Failed to list portal jobs: ${error.response?.data?.message || error.message}`);
  }
}

module.exports = {
  createJob,
  updateJob,
  deleteJob,
  toggleJobPause,
  runJobNow,
  getJobStatus,
  listJobRuns,
  getRunDetails,
  cancelRun,
  getRunOutput,
  listPortalJobs
};
