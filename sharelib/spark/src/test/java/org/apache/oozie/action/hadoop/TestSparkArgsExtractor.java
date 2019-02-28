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

import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;
import org.junit.After;

import org.apache.hadoop.fs.Path;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TestSparkArgsExtractor {

    @Test
    public void testAppendOoziePropertiesToSparkConf() throws Exception {
        final List<String> sparkArgs = new ArrayList<>();
        final Configuration actionConf = new Configuration();
        actionConf.set("foo", "foo-not-to-include");
        actionConf.set("oozie.launcher", "launcher-not-to-include");
        actionConf.set("oozie.spark", "spark-not-to-include");
        actionConf.set("oozie.bar", "bar");

        new SparkArgsExtractor(actionConf).appendOoziePropertiesToSparkConf(sparkArgs);

        assertEquals(Lists.newArrayList("--conf", "spark.oozie.bar=bar"), sparkArgs);
    }

    @Test
    public void testLocalClientArgsParsing() throws Exception {
        final Configuration actionConf = new Configuration();
        actionConf.set(SparkActionExecutor.SPARK_MASTER, "local[*]");
        actionConf.set(SparkActionExecutor.SPARK_MODE, "client");
        actionConf.set(SparkActionExecutor.SPARK_CLASS, "org.apache.oozie.example.SparkFileCopy");
        actionConf.set(SparkActionExecutor.SPARK_JOB_NAME, "Spark Copy File");
        actionConf.set(SparkActionExecutor.SPARK_OPTS, "--driver-memory  1042M " +
                "--conf spark.executor.extraClassPath=aaa " +
                "--conf user.property.after.spark.executor.extraClassPath=bbb " +
                "--conf spark.driver.extraClassPath=ccc " +
                "--conf user.property.after.spark.driver.extraClassPath=ddd " +
                "--conf spark.executor.extraJavaOptions=\"-XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=/tmp\"");
        actionConf.set(SparkActionExecutor.SPARK_JAR, "/lib/test.jar");

        final String[] mainArgs = {"arg0", "arg1"};
        final List<String> sparkArgs = new SparkArgsExtractor(actionConf).extract(mainArgs);

        assertEquals("Spark args mismatch",
                Lists.newArrayList("--master", "local[*]",
                        "--deploy-mode", "client",
                        "--name", "Spark Copy File",
                        "--class", "org.apache.oozie.example.SparkFileCopy",
                        "--driver-memory", "1042M",
                        "--conf", "spark.executor.extraClassPath=aaa",
                        "--conf", "user.property.after.spark.executor.extraClassPath=bbb",
                        "--conf", "spark.driver.extraClassPath=ccc",
                        "--conf", "user.property.after.spark.driver.extraClassPath=ddd",
                        "--conf", "spark.executor.extraJavaOptions=-XX:+HeapDumpOnOutOfMemoryError " +
                                "-XX:HeapDumpPath=/tmp -Dlog4j.configuration=spark-log4j.properties",
                        "--conf", "spark.yarn.security.tokens.hive.enabled=false",
                        "--conf", "spark.yarn.security.tokens.hbase.enabled=false",
                        "--conf", "spark.driver.extraJavaOptions=-Dlog4j.configuration=spark-log4j.properties",
                        "--verbose",
                        "/lib/test.jar",
                        "arg0",
                        "arg1"),
                sparkArgs);
    }

    @Test
    public void testYarnClientExecutorAndDriverExtraClasspathsArgsParsing() throws Exception {
        final Configuration actionConf = new Configuration();
        actionConf.set(SparkActionExecutor.SPARK_MASTER, "yarn");
        actionConf.set(SparkActionExecutor.SPARK_MODE, "client");
        actionConf.set(SparkActionExecutor.SPARK_CLASS, "org.apache.oozie.example.SparkFileCopy");
        actionConf.set(SparkActionExecutor.SPARK_JOB_NAME, "Spark Copy File");
        actionConf.set(SparkActionExecutor.SPARK_OPTS, "--driver-memory  1042M " +
                "--conf spark.executor.extraClassPath=aaa " +
                "--conf user.property.after.spark.executor.extraClassPath=bbb " +
                "--conf spark.driver.extraClassPath=ccc " +
                "--conf user.property.after.spark.driver.extraClassPath=ddd " +
                "--conf spark.executor.extraJavaOptions=\"-XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=/tmp\"");
        actionConf.set(SparkActionExecutor.SPARK_JAR, "/lib/test.jar");

        final String[] mainArgs = {"arg0", "arg1"};
        final List<String> sparkArgs = new SparkArgsExtractor(actionConf).extract(mainArgs);

        assertEquals("Spark args mismatch",
                Lists.newArrayList("--master", "yarn",
                        "--deploy-mode", "client",
                        "--name", "Spark Copy File",
                        "--class", "org.apache.oozie.example.SparkFileCopy",
                        "--driver-memory", "1042M",
                        "--conf", "user.property.after.spark.executor.extraClassPath=bbb",
                        "--conf", "user.property.after.spark.driver.extraClassPath=ddd",
                        "--conf", "spark.executor.extraJavaOptions=-XX:+HeapDumpOnOutOfMemoryError " +
                                "-XX:HeapDumpPath=/tmp -Dlog4j.configuration=spark-log4j.properties",
                        "--conf", "spark.executor.extraClassPath=aaa:$PWD/*",
                        "--conf", "spark.driver.extraClassPath=ccc:$PWD/*",
                        "--conf", "spark.yarn.security.tokens.hive.enabled=false",
                        "--conf", "spark.yarn.security.tokens.hbase.enabled=false",
                        "--conf", "spark.driver.extraJavaOptions=-Dlog4j.configuration=spark-log4j.properties",
                        "--files", "spark-log4j.properties,hive-site.xml",
                        "--conf", "spark.yarn.jar=null",
                        "--verbose",
                        "/lib/test.jar",
                        "arg0",
                        "arg1"),
                sparkArgs);
    }

    @Test
    public void testYarnClientFilesAndArchivesArgsParsing() throws Exception {
        final Configuration actionConf = new Configuration();
        actionConf.set(SparkActionExecutor.SPARK_MASTER, "yarn");
        actionConf.set(SparkActionExecutor.SPARK_MODE, "client");
        actionConf.set(SparkActionExecutor.SPARK_CLASS, "org.apache.oozie.example.SparkFileCopy");
        actionConf.set(SparkActionExecutor.SPARK_JOB_NAME, "Spark Copy File");
        actionConf.set(SparkActionExecutor.SPARK_OPTS, "--files aaa " +
                "--archives bbb " +
                "--files=ccc " +
                "--archives=ddd");
        actionConf.set(SparkActionExecutor.SPARK_JAR, "/lib/test.jar");

        final String[] mainArgs = {"arg0", "arg1"};
        final List<String> sparkArgs = new SparkArgsExtractor(actionConf).extract(mainArgs);

        assertEquals("Spark args mismatch",
                Lists.newArrayList("--master", "yarn",
                        "--deploy-mode", "client",
                        "--name", "Spark Copy File",
                        "--class", "org.apache.oozie.example.SparkFileCopy",
                        "--conf", "spark.executor.extraClassPath=$PWD/*",
                        "--conf", "spark.driver.extraClassPath=$PWD/*",
                        "--conf", "spark.yarn.security.tokens.hive.enabled=false",
                        "--conf", "spark.yarn.security.tokens.hbase.enabled=false",
                        "--conf", "spark.executor.extraJavaOptions=-Dlog4j.configuration=spark-log4j.properties",
                        "--conf", "spark.driver.extraJavaOptions=-Dlog4j.configuration=spark-log4j.properties",
                        "--files", "spark-log4j.properties,hive-site.xml,aaa,ccc",
                        "--archives", "bbb,ddd",
                        "--conf", "spark.yarn.jar=null",
                        "--verbose",
                        "/lib/test.jar",
                        "arg0",
                        "arg1"),
                sparkArgs);
    }

    @Test
    public void testDriverClassPathArgsParsing() throws Exception {
        final Configuration actionConf = new Configuration();
        actionConf.set(SparkActionExecutor.SPARK_MASTER, "yarn");
        actionConf.set(SparkActionExecutor.SPARK_MODE, "client");
        actionConf.set(SparkActionExecutor.SPARK_CLASS, "org.apache.oozie.example.SparkFileCopy");
        actionConf.set(SparkActionExecutor.SPARK_JOB_NAME, "Spark Copy File");
        actionConf.set(SparkActionExecutor.SPARK_OPTS, "--driver-class-path aaa");
        actionConf.set(SparkActionExecutor.SPARK_JAR, "/lib/test.jar");

        final String[] mainArgs = {"arg0", "arg1"};
        final List<String> sparkArgs = new SparkArgsExtractor(actionConf).extract(mainArgs);

        assertEquals("Spark args mismatch",
                Lists.newArrayList("--master", "yarn",
                        "--deploy-mode", "client",
                        "--name", "Spark Copy File",
                        "--class", "org.apache.oozie.example.SparkFileCopy",
                        "--conf", "spark.executor.extraClassPath=$PWD/*",
                        "--conf", "spark.driver.extraClassPath=aaa:$PWD/*",
                        "--conf", "spark.yarn.security.tokens.hive.enabled=false",
                        "--conf", "spark.yarn.security.tokens.hbase.enabled=false",
                        "--conf", "spark.executor.extraJavaOptions=-Dlog4j.configuration=spark-log4j.properties",
                        "--conf", "spark.driver.extraJavaOptions=-Dlog4j.configuration=spark-log4j.properties",
                        "--files", "spark-log4j.properties,hive-site.xml",
                        "--conf", "spark.yarn.jar=null",
                        "--verbose",
                        "/lib/test.jar",
                        "arg0",
                        "arg1"),
                sparkArgs);
    }

    private static final String LOCAL_FILE = "/tmp/local_file.txt#local_file.txt";
    private static final String HDFS_FILE = "hdfs:///hadoop/file.pq#file.pq";

    @Test
    public void testHashMarkInFilesPath() throws OozieActionConfiguratorException, IOException, URISyntaxException {
        doTestingHashMarkInPath("--files");
    }

    @Test
    public void testHashMarkInArchivesPath() throws OozieActionConfiguratorException, IOException, URISyntaxException {
        doTestingHashMarkInPath("--archives");
    }

    private void doTestingHashMarkInPath(final String option)
            throws OozieActionConfiguratorException, IOException, URISyntaxException {
        Configuration actionConf = createSparkActionConfWithCustomSparkOpts(
                String.format("%s %s,%s", option, LOCAL_FILE, HDFS_FILE));
        List<String> sparkArgs = new SparkArgsExtractor(actionConf).extract(new String[0]);
        assertForFilePaths(sparkArgs, LOCAL_FILE, option);
        assertForFilePaths(sparkArgs, HDFS_FILE, option);

        actionConf = createSparkActionConfWithCustomSparkOpts(
                String.format("%s=%s,%s", option, LOCAL_FILE, HDFS_FILE));
        sparkArgs = new SparkArgsExtractor(actionConf).extract(new String[0]);
        assertForFilePaths(sparkArgs, LOCAL_FILE, option);
        assertForFilePaths(sparkArgs, HDFS_FILE, option);
    }

    @Test
    public void testIfUrisAreDecoded() {
        final SparkArgsExtractor extractor = new SparkArgsExtractor(new Configuration());
        final Collection<String> result = extractor.decodeUriPaths(Arrays.asList(
                new Path(LOCAL_FILE).toUri(),
                new Path(HDFS_FILE).toUri()
        ));

        assertTrue(result + " shall contain " + LOCAL_FILE, result.contains(LOCAL_FILE));
        assertTrue(result + " shall contain " + HDFS_FILE, result.contains(HDFS_FILE));
    }

    @Test
    public void testDecodeUriPathsNullInput() {
        assertTrue("In case providing empty or null input, empty list shall be returned",
                new SparkArgsExtractor(new Configuration()).decodeUriPaths(null).isEmpty());
    }

    private void assertForFilePaths(final List<String> collection, final String path, final String option) {
        final String positive = path;
        final String negative = path.replace("#", "%23");

        final Iterator<String> iterator = collection.iterator();
        while (iterator.hasNext()) {
            String elem = iterator.next();
            if (elem != null && elem.equals(option)) {
                elem = iterator.next();
                assertTrue(positive + " shall be present in " + collection, elem.contains(positive));
                assertFalse(negative + " shall not be present in " + collection, elem.contains(negative));
                return;
            }
        }
        fail(String.format("Neither %s nor %s present in %s.", positive, negative, collection));
    }

    private Configuration createSparkActionConfWithCustomSparkOpts(final String sparkOpts) {
        final Configuration actionConf = new Configuration();
        actionConf.set(SparkActionExecutor.SPARK_MASTER, "yarn");
        actionConf.set(SparkActionExecutor.SPARK_MODE, "client");
        actionConf.set(SparkActionExecutor.SPARK_CLASS, "org.apache.oozie.example.SparkFileCopy");
        actionConf.set(SparkActionExecutor.SPARK_JAR, "/lib/test.jar");

        actionConf.set(SparkActionExecutor.SPARK_OPTS, sparkOpts);
        return actionConf;
    }
}