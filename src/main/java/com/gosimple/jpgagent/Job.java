/*
 * Copyright (c) 2016, Adam Brusselback
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package com.gosimple.jpgagent;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class Job implements CancellableRunnable
{
    private final int job_id;
    private int job_log_id;
    private JobStatus job_status;
    final List<JobStep> job_step_list = new ArrayList<>();
    private static final Map<JobStep, Future> future_map = new HashMap<>();

    public Job(final int job_id)
    {
        this.job_id = job_id;
        Config.INSTANCE.logger.debug("Instantiating Job begin.");

        final String log_sql =
                "INSERT INTO pgagent.pga_joblog(jlgjobid, jlgstatus) " +
                        "VALUES (?, 'r') " +
                        "RETURNING jlgid;";
        Config.INSTANCE.logger.debug("Inserting logging and marking job as being worked on.");
        try (final PreparedStatement log_statement = Database.INSTANCE.getMainConnection().prepareStatement(log_sql))
        {

            log_statement.setInt(1, this.job_id);
            try (final ResultSet resultSet = log_statement.executeQuery())
            {
                while (resultSet.next())
                {
                    job_log_id = resultSet.getInt("jlgid");
                }
            }
        }
        catch (final SQLException e)
        {
            Config.INSTANCE.logger.error(e.getMessage());
        }

        final String step_sql =
                "SELECT jstid " +
                        ", jstjobid " +
                        ", jstname " +
                        ", jstdesc " +
                        ", jstkind " +
                        ", jstcode " +
                        ", jstconnstr " +
                        ", jstdbname " +
                        ", jstonerror " +
                        "FROM pgagent.pga_jobstep " +
                        "WHERE jstenabled " +
                        "AND jstjobid=? " +
                        "ORDER BY jstname, jstid";

        Config.INSTANCE.logger.debug("Building steps.");
        try (final PreparedStatement statement = Database.INSTANCE.getMainConnection().prepareStatement(step_sql))
        {
            statement.setInt(1, job_id);

            try (final ResultSet resultSet = statement.executeQuery())
            {
                while (resultSet.next())
                {
                    JobStep job_step = new JobStep(
                            this.job_log_id,
                            resultSet.getInt("jstjobid"),
                            resultSet.getInt("jstid"),
                            resultSet.getString("jstname"),
                            resultSet.getString("jstdesc"),
                            JobStep.StepType.convertTo(resultSet.getString("jstkind")),
                            resultSet.getString("jstcode"),
                            resultSet.getString("jstconnstr"),
                            resultSet.getString("jstdbname"),
                            JobStep.OnError.convertTo(resultSet.getString("jstonerror"))
                    );
                    job_step_list.add(job_step);
                }
            }
        }
        catch (final SQLException e)
        {
            Config.INSTANCE.logger.error("An error occurred getting job steps.");
            Config.INSTANCE.logger.error(e.getMessage());
        }
        Config.INSTANCE.logger.debug("Job instantiation complete.");
    }


    public void run()
    {
        Config.INSTANCE.logger.info("Job id: {} started.", job_id);
        boolean failed_step = false;
        try
        {
            for (JobStep job_step : job_step_list)
            {
                future_map.put(job_step, ThreadFactory.INSTANCE.submitTask(job_step));
                // Block until step is done so we can execute in-order.
                try
                {
                    future_map.get(job_step).get();
                }
                catch (ExecutionException e)
                {
                    Config.INSTANCE.logger.error(e.getMessage());
                }

            }


            for (JobStep job_step : job_step_list)
            {
                if (job_step.getStepStatus().equals(JobStep.StepStatus.FAIL)
                        && job_step.getOnError().equals(JobStep.OnError.FAIL))
                {
                    failed_step = true;
                }
            }
        }
        catch (InterruptedException e)
        {
            job_status = JobStatus.ABORTED;
        }
        if (job_status == null && job_step_list.isEmpty())
        {
            job_status = JobStatus.IGNORE;
        }
        else if (job_status == null && failed_step)
        {
            job_status = JobStatus.FAIL;
        }
        else if (job_status == null)
        {
            job_status = JobStatus.SUCCEED;
        }

        final String update_log_sql =
                "UPDATE pgagent.pga_joblog SET jlgstatus = ?, jlgduration=now() - jlgstart " +
                        "WHERE jlgid = ?;";
        final String update_job_sql =
                "UPDATE pgagent.pga_job SET jobagentid=NULL, jobnextrun=NULL " +
                        "WHERE jobid = ?;";
        try (final PreparedStatement update_job_statement = Database.INSTANCE.getMainConnection().prepareStatement(update_job_sql);
             final PreparedStatement update_log_statement = Database.INSTANCE.getMainConnection().prepareStatement(update_log_sql))
        {
            update_job_statement.setInt(1, job_id);
            update_job_statement.execute();

            update_log_statement.setString(1, this.job_status.getDbRepresentation());
            update_log_statement.setInt(2, this.job_log_id);
            update_log_statement.execute();
        }
        catch (SQLException e)
        {
            Config.INSTANCE.logger.error(e.getMessage());
        }
        Config.INSTANCE.logger.info("Job id: {} complete.", job_id);
    }

    /**
     * Should stop any long running process the thread was doing to exit gracefully as quickly as possible.
     */
    @Override
    public void cancelTask()
    {
        for (Future future : future_map.values())
        {
            if (!future.isDone())
            {
                future.cancel(true);
            }
        }
    }

    protected enum JobStatus
    {
        RUNNING("r"),
        FAIL("f"),
        SUCCEED("s"),
        ABORTED("d"),
        IGNORE("i");

        private final String db_representation;

        private JobStatus(final String db_representation)
        {
            this.db_representation = db_representation;
        }

        public static JobStatus convertTo(final String db_string)
        {
            for (JobStatus job_status : JobStatus.values())
            {
                if (db_string.equals(job_status.db_representation))
                {
                    return job_status;
                }
            }
            return null;
        }

        public String getDbRepresentation()
        {
            return db_representation;
        }
    }
}
