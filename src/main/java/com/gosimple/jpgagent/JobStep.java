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

import org.apache.commons.lang3.SystemUtils;

import java.io.*;
import java.sql.*;
import java.util.Map;

public class JobStep implements CancellableRunnable
{
    private final int job_log_id;
    private int job_step_log_id;
    private StepStatus step_status;
    private int step_result;
    private String step_output;
    private final int job_id;
    private final int step_id;
    private final String step_name;
    private final String step_description;
    private final StepType step_type;
    private String code;
    private final String connection_string;
    private final String database_name;
    private final OnError on_error;
    private OSType os_type;

    private Statement running_statement;
    private Process running_process;
    private Long start_time;

    /*
    * Annotation set parameters.
     */
    // If true, will run in parallel with previous step.
    private Boolean run_in_parallel = false;
    // Timeout setting to abort job if running longer than this value.
    private Long job_step_timeout = null;

    public JobStep(final int job_log_id, final int job_id, final int step_id, final String step_name, final String step_description, final StepType step_type, final String code, final String connection_string, final String database_name, final OnError on_error)
    {
        Config.INSTANCE.logger.debug("JobStep instantiation begin.");
        this.job_log_id = job_log_id;
        this.job_id = job_id;
        this.step_id = step_id;
        this.step_name = step_name;
        this.step_description = step_description;
        this.step_type = step_type;
        this.code = code;
        this.connection_string = connection_string;
        this.database_name = database_name;
        this.on_error = on_error;
        if (SystemUtils.IS_OS_WINDOWS)
        {
            os_type = OSType.WIN;
        }
        else if (SystemUtils.IS_OS_LINUX || SystemUtils.IS_OS_MAC)
        {
            os_type = OSType.NIX;
        }

        /*
         Assign any values from annotations.
          */
        try
        {
            Map<String, String> annotations = AnnotationUtil.parseAnnotations(step_description);
            if(annotations.containsKey(JobStepAnnotations.RUN_IN_PARALLEL.name()))
            {
                run_in_parallel = AnnotationUtil.parseValue(JobStepAnnotations.RUN_IN_PARALLEL, annotations.get(JobStepAnnotations.RUN_IN_PARALLEL.name()), Boolean.class);
            }
            if(annotations.containsKey(JobStepAnnotations.JOB_STEP_TIMEOUT.name()))
            {
                job_step_timeout = AnnotationUtil.parseValue(JobStepAnnotations.JOB_STEP_TIMEOUT, annotations.get(JobStepAnnotations.JOB_STEP_TIMEOUT.name()), Long.class);
            }
        }
        catch (Exception e)
        {
            Config.INSTANCE.logger.error("An issue with the annotations on job has stopped them from being processed.");
        }
        Config.INSTANCE.logger.debug("JobStep instantiation complete.");
    }

    public void run()
    {
        this.start_time = System.currentTimeMillis();
        final String log_sql =
                "INSERT INTO pgagent.pga_jobsteplog(jsljlgid, jsljstid, jslstatus) " +
                "SELECT ?, ?, ? " +
                "RETURNING jslid;";
        try (final PreparedStatement log_statement = Database.INSTANCE.getMainConnection().prepareStatement(log_sql))
        {
            log_statement.setInt(1, this.job_log_id);
            log_statement.setInt(2, this.step_id);
            log_statement.setString(3, StepStatus.RUNNING.getDbRepresentation());
            try (ResultSet resultSet = log_statement.executeQuery())
            {
                while (resultSet.next())
                {
                    job_step_log_id = resultSet.getInt("jslid");
                }
            }
        }
        catch (final SQLException e)
        {
            Config.INSTANCE.logger.error(e.getMessage());
        }
        switch (step_type)
        {
            case SQL:
            {
                Config.INSTANCE.logger.debug("Executing SQL step: {}", step_id);

                try (Connection connection = Database.INSTANCE.getConnection(database_name))
                {
                    try (Statement statement = connection.createStatement())
                    {
                        this.running_statement = statement;
                        statement.execute(code);
                        this.running_statement = null;
                        step_result = 1;
                        step_status = StepStatus.SUCCEED;
                    }
                }
                catch (final Exception e)
                {
                    step_output = e.getMessage();
                    if (Thread.currentThread().isInterrupted())
                    {
                        step_result = 0;
                        step_status = StepStatus.ABORTED;
                    }
                    else if (on_error.equals(OnError.FAIL))
                    {
                        step_result = -1;
                        step_status = StepStatus.FAIL;
                    }
                    else if (on_error.equals(OnError.IGNORE))
                    {
                        step_result = -1;
                        step_status = StepStatus.IGNORE;
                    }
                    else if (on_error.equals(OnError.SUCCEED))
                    {
                        step_result = -1;
                        step_status = StepStatus.SUCCEED;
                    }
                }
                Config.INSTANCE.logger.debug("SQL step: {} completed successfully.", step_id);
                break;
            }
            case BATCH:
            {
                Config.INSTANCE.logger.debug("Executing Batch step: {}", step_id);

                try
                {
                    // Replace line breaks for each OS type.
                    code = code.replaceAll("\\r\\n|\\r|\\n", System.getProperty("line.separator"));

                    final String fileExtension;
                    if (os_type.equals(OSType.WIN))
                    {
                        fileExtension = ".bat";
                    }
                    else
                    {
                        fileExtension = ".sh";
                    }

                    final File tmp_file_script = File.createTempFile("pga_", fileExtension, null);
                    tmp_file_script.deleteOnExit();
                    tmp_file_script.setWritable(true);
                    tmp_file_script.setExecutable(true);


                    final BufferedWriter buffered_writer = new BufferedWriter(new FileWriter(tmp_file_script));
                    buffered_writer.write(this.code);
                    buffered_writer.close();

                    final ProcessBuilder process_builder = new ProcessBuilder(tmp_file_script.getAbsolutePath());
                    this.running_process = process_builder.start();
                    this.running_process.waitFor();


                    final BufferedReader buffered_reader = new BufferedReader(new InputStreamReader(this.running_process.getInputStream()));
                    final StringBuilder string_builder = new StringBuilder();
                    String line = null;
                    while ((line = buffered_reader.readLine()) != null)
                    {
                        string_builder.append(line);
                        string_builder.append(System.getProperty("line.separator"));
                    }
                    tmp_file_script.delete();
                    this.step_output = string_builder.toString();
                    this.step_status = StepStatus.SUCCEED;
                    this.step_result = running_process.exitValue();
                }
                catch (InterruptedException e)
                {
                    this.step_result = running_process.exitValue();
                    this.step_status = StepStatus.ABORTED;
                }
                catch (Exception e)
                {
                    this.step_result = running_process.exitValue();
                    if (this.on_error.equals(OnError.FAIL))
                    {
                        this.step_status = StepStatus.FAIL;
                    }
                    else if (this.on_error.equals(OnError.IGNORE))
                    {
                        this.step_status = StepStatus.IGNORE;
                    }
                    else if (this.on_error.equals(OnError.SUCCEED))
                    {
                        this.step_status = StepStatus.SUCCEED;
                    }
                }
                finally
                {
                    this.running_process = null;
                }
                Config.INSTANCE.logger.debug("Batch step: {} completed successfully.", step_id);
                break;
            }
        }

        final String update_log_sql =
                "UPDATE pgagent.pga_jobsteplog " +
                        "SET jslduration = now() - jslstart, " +
                        "jslstatus = ?, " +
                        "jslresult = ?, " +
                        "jsloutput = ? " +
                        "WHERE jslid=?";
        try (PreparedStatement update_log_statement = Database.INSTANCE.getMainConnection().prepareStatement(update_log_sql))
        {
            update_log_statement.setString(1, this.step_status.getDbRepresentation());
            update_log_statement.setInt(2, this.step_result);
            update_log_statement.setString(3, this.step_output);
            update_log_statement.setInt(4, this.job_step_log_id);
            update_log_statement.execute();
        }
        catch (final SQLException e)
        {
            Config.INSTANCE.logger.error(e.getMessage());
        }
    }

    /**
     * Returns if the job is timed out or not.
     * @return
     */
    public boolean isTimedOut()
    {
        if(null != job_step_timeout && null != start_time)
        {
            return System.currentTimeMillis() - start_time > job_step_timeout;
        }
        else
        {
            return false;
        }
    }

    /**
     * Should stop any long running process the thread was doing to exit gracefully as quickly as possible.
     */
    @Override
    public void cancelTask()
    {
        switch (step_type)
        {
            case SQL:
                if (running_statement != null)
                {
                    try
                    {
                        running_statement.cancel();
                    }
                    catch (SQLException e)
                    {
                        Config.INSTANCE.logger.error(e.getMessage());
                    }
                }
                break;
            case BATCH:
                if (running_process != null && running_process.isAlive())
                {
                    running_process.destroy();
                }
                break;
        }

    }

    /**
     * Gets the StepStatus of the JobStep.
     *
     * @return
     */
    public StepStatus getStepStatus()
    {
        return step_status;
    }

    /**
     * Gets the OnError of the JobStep.
     *
     * @return
     */
    public OnError getOnError()
    {
        return on_error;
    }

    /**
     * Returns if the job can run in parallel with the previous step.
     * @return
     */
    public Boolean canRunInParallel()
    {
        return this.run_in_parallel;
    }

    protected enum StepType
    {
        SQL("s"),
        BATCH("b");

        private final String db_representation;

        private StepType(final String db_representation)
        {
            this.db_representation = db_representation;
        }

        public static StepType convertTo(final String db_string)
        {
            for (StepType step_type : StepType.values())
            {
                if (db_string.equals(step_type.db_representation))
                {
                    return step_type;
                }
            }
            return null;
        }

        public String getDbRepresentation()
        {
            return db_representation;
        }
    }

    protected enum OnError
    {
        FAIL("f"),
        SUCCEED("s"),
        IGNORE("i");

        private final String db_representation;

        private OnError(String db_representation)
        {
            this.db_representation = db_representation;
        }

        public static OnError convertTo(final String db_string)
        {
            for (final OnError on_error : OnError.values())
            {
                if (db_string.equals(on_error.db_representation))
                {
                    return on_error;
                }
            }
            return null;
        }

        public String getDbRepresentation()
        {
            return db_representation;
        }
    }

    protected enum StepStatus
    {
        RUNNING("r"),
        FAIL("f"),
        SUCCEED("s"),
        ABORTED("d"),
        IGNORE("i");

        private final String db_representation;

        private StepStatus(final String db_representation)
        {
            this.db_representation = db_representation;
        }

        public static StepStatus convertTo(final String db_string)
        {
            for (final StepStatus step_status : StepStatus.values())
            {
                if (db_string.equals(step_status.db_representation))
                {
                    return step_status;
                }
            }
            return null;
        }

        public String getDbRepresentation()
        {
            return db_representation;
        }
    }

    protected enum OSType
    {
        WIN,
        NIX;
    }

    public enum JobStepAnnotations implements AnnotationDefinition
    {
        RUN_IN_PARALLEL(Boolean.class),
        JOB_STEP_TIMEOUT(Long.class);

        final Class<?> annotation_value_type;

        private JobStepAnnotations(final Class annotation_value_type)
        {
            this.annotation_value_type = annotation_value_type;
        }

        @Override
        public Class<?> getAnnotationValueType()
        {
            return this.annotation_value_type;
        }
    }
}
