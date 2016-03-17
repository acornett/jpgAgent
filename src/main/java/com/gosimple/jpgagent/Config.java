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

import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public enum Config
{
    INSTANCE;

    // Create a logger.
    protected final Logger logger = LoggerFactory.getLogger("jpgAgent");
    // Host name for the system running jpgAgent.
    protected String hostname;

    @Option(name = "-h", required = true, usage = "Database host address.")
    protected String db_host;
    @Option(name = "--port", required = false, usage = "Database host port.")
    protected int db_port = 5432;
    @Option(name = "-u", required = true, usage = "Database user.")
    protected String db_user;
    @Option(name = "-p", required = true, usage = "Database password.")
    protected String db_password;
    @Option(name = "-d", required = true, usage = "jPGAgent database.")
    protected String db_database;
    @Option(name = "-t", required = false, usage = "Job poll interval (ms).")
    protected long job_poll_interval = 10000;
    @Option(name = "-r", required = false, usage = "Connection retry interval (ms).")
    protected long connection_retry_interval = 30000;
}
