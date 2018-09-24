/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.oozie.action.hadoop;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URL;
import java.util.Map;
import java.util.Properties;

import java.io.Writer;

import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.PropertyConfigurator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.sqoop.Sqoop;

import com.google.common.annotations.VisibleForTesting;

public class SqoopMain extends LauncherMain {

    public static final String SQOOP_SITE_CONF = "sqoop-site.xml";

    @VisibleForTesting
    static final Pattern[] SQOOP_JOB_IDS_PATTERNS = {
            Pattern.compile("Job complete: (job_\\S*)"),
            Pattern.compile("Job (job_\\S*) has completed successfully"),
            Pattern.compile("Submitted application (application[0-9_]*)")
    };

    private static final String SQOOP_LOG4J_PROPS = "sqoop-log4j.properties";
    private static final String SQOOP_LOG4J2_XML = "sqoop-log4j2.xml";
    public static final String TEZ_CREDENTIALS_PATH = "tez.credentials.path";
    public static final String OOZIE_ACTION_CONF_XML = "oozie.action.conf.xml";

    public static void main(String[] args) throws Exception {
        run(SqoopMain.class, args);
    }

    private static Configuration initActionConf() {
        // loading action conf prepared by Oozie
        Configuration sqoopConf = new Configuration(false);

        String actionXml = System.getProperty(OOZIE_ACTION_CONF_XML);

        if (actionXml == null) {
            throw new RuntimeException(String.format("Missing Java System Property [%s]", OOZIE_ACTION_CONF_XML));
        }
        if (!new File(actionXml).exists()) {
            throw new RuntimeException("Action Configuration XML file [" + actionXml + "] does not exist");
        }

        sqoopConf.addResource(new Path("file:///", actionXml));
        setYarnTag(sqoopConf);

        String delegationToken = getFilePathFromEnv("HADOOP_TOKEN_FILE_LOCATION");
        if (delegationToken != null) {
            sqoopConf.setBoolean("sqoop.hbase.security.token.skip", true);
            sqoopConf.set("mapreduce.job.credentials.binary", delegationToken);
            sqoopConf.set(TEZ_CREDENTIALS_PATH, delegationToken);
            System.out.println("------------------------");
            System.out.println("Setting env property for mapreduce.job.credentials.binary to: " + delegationToken);
            System.out.println("------------------------");
            System.setProperty("mapreduce.job.credentials.binary", delegationToken);
            System.out.println("------------------------");
            System.out.println("Setting env property for tez.credentials.path to: " + delegationToken);
            System.out.println("------------------------");
            System.setProperty(TEZ_CREDENTIALS_PATH, delegationToken);
        } else {
            System.out.println("Non-Kerberos execution");
        }

        return sqoopConf;
    }

    public static Configuration setUpSqoopSite() throws Exception {
        Configuration sqoopConf = initActionConf();

        // Write the action configuration out to sqoop-site.xml
        createFileWithContentIfNotExists(SQOOP_SITE_CONF, sqoopConf);
        logMasking("Sqoop Configuration Properties:", sqoopConf);
        return sqoopConf;
    }
    private String setUpSqoopLog4J(final String rootLogLevel, final String logLevel) throws IOException {
        System.out.println("Setting up log4j");
        String logFile = getSqoopLogFile();

        log4jProperties.setProperty("log4j.rootLogger", rootLogLevel + ", A");
        log4jProperties.setProperty("log4j.logger.org.apache.sqoop", logLevel + ", A");
        log4jProperties.setProperty("log4j.additivity.org.apache.sqoop", "false");
        log4jProperties.setProperty("log4j.appender.A", "org.apache.log4j.ConsoleAppender");
        log4jProperties.setProperty("log4j.appender.A.layout", "org.apache.log4j.PatternLayout");
        log4jProperties.setProperty("log4j.appender.A.layout.ConversionPattern", "%d [%t] %-5p %c %x - %m%n");
        log4jProperties.setProperty("log4j.appender.jobid", "org.apache.log4j.FileAppender");
        log4jProperties.setProperty("log4j.appender.jobid.file", logFile);
        log4jProperties.setProperty("log4j.appender.jobid.layout", "org.apache.log4j.PatternLayout");
        log4jProperties.setProperty("log4j.appender.jobid.layout.ConversionPattern", "%d [%t] %-5p %c %x - %m%n");
        log4jProperties.setProperty("log4j.logger.org.apache.hadoop.mapred", "INFO, jobid, A");
        log4jProperties.setProperty("log4j.logger.org.apache.sqoop", String.format("%s, jobid, A", logLevel));
        log4jProperties.setProperty("log4j.logger.org.apache.hadoop.mapreduce.Job", "INFO, jobid, A");
        log4jProperties.setProperty("log4j.logger.org.apache.hadoop.yarn.client.api.impl.YarnClientImpl", "INFO, jobid");


        String localProps = new File(SQOOP_LOG4J_PROPS).getAbsolutePath();
        createFileWithContentIfNotExists(localProps, log4jProperties);
        PropertyConfigurator.configure(SQOOP_LOG4J_PROPS);

        System.out.printf("log4j2 configuration file created at %s%n", localProps);
        return logFile;
    }

    private String setUpSqoopLog4J2(final String rootLogLevel) throws IOException {
        System.out.println("Setting up log4j2");

        final String logFile = getSqoopLogFile();
        final File log4j2Xml = new File(SQOOP_LOG4J2_XML);
        try (final Writer writer = new FileWriter(log4j2Xml)) {
            final String logj2SettingsXml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
                    "<Configuration status=\"WARN\">\n" +
                    "    <Appenders>\n" +
                    "        <Console name=\"Console\" target=\"SYSTEM_OUT\">\n" +
                    "            <PatternLayout pattern=\"%d{HH:mm:ss.SSS} [%t] %-5level %logger{36} - %msg%n\"/>\n" +
                    "        </Console>\n" +
                    "        <File name=\"File\" fileName=\"" + logFile + "\">  \n" +
                    "            <PatternLayout pattern=\"%d{HH:mm:ss.SSS} [%t] %-5level %logger{36} - %msg%n\"/>\n" +
                    "        </File> \n" +
                    "    </Appenders>\n" +
                    "    <Loggers>\n" +
                    "        <Root level=\"" + rootLogLevel.toLowerCase() + "\">\n" +
                    "            <AppenderRef ref=\"Console\"/>\n" +
                    "            <AppenderRef ref=\"File\"/>\n" +
                    "        </Root>\n" +
                    "    </Loggers>\n" +
                    "</Configuration>";
            writer.write(logj2SettingsXml);
        }

        System.out.printf("log4j2 configuration file created at %s%n", log4j2Xml.getAbsolutePath());

        final LoggerContext context = (LoggerContext) LogManager.getContext(false);
        context.setConfigLocation(log4j2Xml.toURI()); // forces log4j2 reconfiguration
        return logFile;
    }

    private String getSqoopLogFile() {
        //Logfile to capture job IDs
        String hadoopJobId = System.getProperty("oozie.launcher.job.id");
        if (hadoopJobId == null) {
            throw new RuntimeException("Launcher Hadoop Job ID system property not set");
        }

        return new File("sqoop-oozie-" + hadoopJobId + ".log").getAbsolutePath();
    }

    @Override
    protected void run(String[] args) throws Exception {
        System.out.println();
        System.out.println("Oozie Sqoop action configuration");
        System.out.println("=================================================================");

        final Configuration sqoopConf = setUpSqoopSite();

        final String logLevel = sqoopConf.get("oozie.sqoop.log.level", "INFO");
        final String rootLogLevel = sqoopConf.get("oozie.action." + LauncherMapper.ROOT_LOGGER_LEVEL, "INFO");


        String logFile;
        // MAPREDUCE-6983 switches to slfj4 & log4j2. Need to setup log4j accordingly
        if (isMapReduceUsingLog4j2()) {
            logFile = setUpSqoopLog4J2(rootLogLevel);
        }
        else {
            logFile = setUpSqoopLog4J(rootLogLevel, logLevel);
        }

        final String[] sqoopArgs = MapReduceMain.getStrings(sqoopConf, SqoopActionExecutor.SQOOP_ARGS);

        if (sqoopArgs == null) {
            throw new RuntimeException("Action Configuration does not have [" + SqoopActionExecutor.SQOOP_ARGS + "] property");
        }

        LauncherMapper.printArgs("Sqoop command arguments :", sqoopArgs);
        LauncherMainHadoopUtils.killChildYarnJobs(sqoopConf);

        System.out.println("=================================================================");
        System.out.println();
        System.out.println(">>> Invoking Sqoop command line now >>>");
        System.out.println();
        System.out.flush();

        try {
            runSqoopJob(sqoopArgs);
        }
        catch (SecurityException ex) {
            if (LauncherSecurityManager.getExitInvoked()) {
                if (LauncherSecurityManager.getExitCode() != 0) {
                    throw ex;
                }
            }
        }
        finally {
            System.out.println("\n<<< Invocation of Sqoop command completed <<<\n");
            // harvesting and recording Hadoop Job IDs
            writeExternalChildIDs(logFile, SQOOP_JOB_IDS_PATTERNS, "Sqoop");
        }
    }

    private boolean isMapReduceUsingLog4j2() throws NoSuchFieldException {
        return org.apache.hadoop.mapreduce.Job.class.getDeclaredField("LOG").getType().
                isAssignableFrom(org.slf4j.Logger.class);
    }

    protected void runSqoopJob(String[] args) throws Exception {
        // running as from the command line
        Sqoop.main(args);
    }
}
