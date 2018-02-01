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

package org.apache.oozie.tools;

import com.jcraft.jsch.jce.TripleDESCBC;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;

public class SparkSharelibFixer {
    final protected String sparkVersion;
    final protected String oozieHome;
    final protected FileSystem fs;
    final protected Path srcPath;
    final protected Path dstPath;
    final protected File srcFile;

    protected File sparkHome;
    protected Path sparkTarget;

    SparkSharelibFixer(final FileSystem fs, final String oozieHome, final Path srcPath, final Path dstPath, final File srcFile) {
        this.fs = fs;
        this.oozieHome = oozieHome;
        this.srcPath = srcPath;
        this.dstPath = dstPath;
        this.srcFile = srcFile;
        this.sparkVersion = getSparkVersion();
    }

    void fixIt() throws IOException {
        System.out.println("Fixing oozie " + sparkVersion + "'s sharelib");

        sparkHome = determineSparkHome();
        if (!sparkHome.exists()) {
            return;
        }
        System.out.println("Spark is locally installed at " + sparkHome);

        renameSharelibSparkDir();

        createSparkTargetDir();

        for(CopyTriple t : filesToCopy()) {
            copyFilesToHdfs(t.sourceDir, t.targetDir, t.condition);
        }
    }

    protected List<CopyTriple> filesToCopy() {
        final List<CopyTriple> list = new ArrayList<>();
        list.add(new CopyTriple(new File(sparkHome, getSparkLibsDir()), sparkTarget, getSparkJarFilesPattern()));
        list.add(new CopyTriple(new File(sparkHome, getPySparkLibsDir()), sparkTarget, getPythonFilesPattern()));
        list.add(new CopyTriple(new File(srcFile, "spark"), sparkTarget, new Condition() {
            @Override
            public boolean matches(final File file) {
                return file.getName().startsWith("oozie-sharelib-spark");
            }
        }));
        list.add(new CopyTriple(new File("/etc/spark/conf"), sparkTarget, new Condition() {
            @Override
            public boolean matches(final File file) {
                return file.getName().equals("hive-site.xml");
            }
        }));
        return list;
    }

    protected void renameSharelibSparkDir() throws IOException {
        System.out.println("Renaming spark to spark_orig in " + dstPath.toString());
        fs.rename(new Path(dstPath, "spark"), new Path(dstPath, "spark_orig"));
    }

    protected File determineSparkHome() throws IOException {
        final String canonicalOozieHome = new File(oozieHome).getCanonicalPath();
        final File spark = new File(canonicalOozieHome, "../../current/" + sparkVersion);

        if(spark.exists()) {
            return spark;
        }

        return new File(canonicalOozieHome, "../" + sparkVersion);
    }

    protected void createSparkTargetDir() throws IOException {
        System.out.println("Creating new " + sparkVersion + " directory in " + dstPath.toString());
        sparkTarget = new Path(dstPath, sparkVersion);
        fs.mkdirs(sparkTarget);
    }

    protected void copyFilesToHdfs(final File sourceDir, final Path targetDir, Condition condition) throws IOException {
        System.out.println("Copying local " + sourceDir.getName() + " files to " + sparkTarget.toString());

        if (sourceDir.exists()) {
            for (final File file : sourceDir.listFiles()) {
                if (condition.matches(file))  {
                    System.out.println("Copying " + file.toString() + " to " + targetDir.toString());
                    fs.copyFromLocalFile(false, new Path(file.toString()), targetDir);
                } else {
                    System.out.println("Ignoring file " + file.toString());
                }
            }
        }
    }

    protected String getSparkVersion() {
        return "spark";
    }

    protected String getSparkLibsDir() {
        return "lib";
    }

    protected String getPySparkLibsDir() {
        return "python/lib";
    }

    protected Condition getSparkJarFilesPattern() {
        return new Condition() {
            @Override
            public boolean matches(final File file) {
                try {
                    return !FileUtils.isSymlink(file) && !file.getName().startsWith("spark-examples");
                } catch (IOException e) {

                }
                return false;
            }
        };
    }

    protected Condition getPythonFilesPattern() {
        return new Condition() {
            @Override
            public boolean matches(final File file) {
                return file.getName().endsWith(".zip") || file.getName().endsWith(".jar");
            }
        };
    }

    interface Condition {
        boolean matches(File file);
    }

    class CopyTriple {
        final File sourceDir;
        final Path targetDir;
        final Condition condition;

        public CopyTriple(final File sourceDir, final Path targetDir, final Condition condition) {
            this.sourceDir = sourceDir;
            this.targetDir = targetDir;
            this.condition = condition;
        }
    }
}
