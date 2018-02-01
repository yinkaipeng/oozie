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

import java.io.File;
import java.io.IOException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class Spark2SharelibFixer extends SparkSharelibFixer {
    Spark2SharelibFixer(final FileSystem fs, final String oozieHome, final Path srcPath, final Path dstPath, final File srcFile) {
        super(fs, oozieHome, srcPath, dstPath, srcFile);
    }

    @Override
    protected void renameSharelibSparkDir() throws IOException {
        System.out.println("In case of spark2, original sharelib should not be renamed.");
    }

    @Override
    protected Condition getPythonFilesPattern() {
        return new Condition() {
            @Override
            public boolean matches(final File file) {
                return file.getName().startsWith("py");
            }
        };
    }

    @Override
    protected String getSparkVersion() {
        return "spark2";
    }

    @Override
    protected String getSparkLibsDir() {
        return "jars";
    }
}
