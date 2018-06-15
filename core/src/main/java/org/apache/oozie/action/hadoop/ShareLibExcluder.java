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

import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.util.XConfiguration;
import org.apache.oozie.util.XLog;

import java.net.URI;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.regex.Pattern;

import static org.apache.oozie.action.hadoop.JavaActionExecutor.ACTION_SHARELIB_FOR;
import static org.apache.oozie.action.hadoop.JavaActionExecutor.SHARELIB_EXCLUDE_SUFFIX;

public class ShareLibExcluder {

    private final URI rootURI;
    private final Pattern configuredExcludePattern;
    private XLog LOG = XLog.getLog(getClass());
    public static final String VALUE_NULL_MSG = "The value of %s cannot be null.";


    public ShareLibExcluder(final Configuration actionConf, final Configuration servicesConf, final Configuration jobConf,
                            final String executorType, final URI shareLibRoot) {
        this.rootURI = shareLibRoot;
        this.configuredExcludePattern = loadAndBuildPattern(actionConf, servicesConf, jobConf, executorType);
    }

    private Pattern loadAndBuildPattern(final Configuration actionConf, final Configuration servicesConf,
                                     final Configuration jobConf, final String executorType) {
        Preconditions.checkNotNull(actionConf, VALUE_NULL_MSG, "actionConf");
        Preconditions.checkNotNull(servicesConf, VALUE_NULL_MSG, "servicesConf");
        Preconditions.checkNotNull(jobConf, VALUE_NULL_MSG, "jobConf");

        String excludeProperty = ACTION_SHARELIB_FOR + executorType + SHARELIB_EXCLUDE_SUFFIX;

        // try to find the excludeValue in one of the configs
        String excludePattern = actionConf.get(excludeProperty);

        if (excludePattern == null) {
            excludePattern = jobConf.get(excludeProperty);
        }

        if (excludePattern == null) {
            excludePattern = servicesConf.get(excludeProperty);
        }

        if (excludePattern == null) {
            LOG.info("Sharelib exclude pattern not configured, skipping.");
            return null;
        }

        actionConf.set(excludeProperty, excludePattern);
        LOG.debug("Setting action configuration property: {0}={1}", excludeProperty, excludePattern);
        LOG.info("The following sharelib exclude pattern will be used: {0}", excludePattern);

        return  Pattern.compile(excludePattern);
    }

    public boolean checkExclude(final URI actionLibURI) {
        Preconditions.checkNotNull(actionLibURI, VALUE_NULL_MSG, "actionLibURI");

        if (configuredExcludePattern != null && rootURI != null) {
            if (configuredExcludePattern.matcher(rootURI.relativize(actionLibURI).getPath()).matches()) {
                LOG.info("Mark file for excluding from distributed cache: {0}", actionLibURI.getPath());
                return true;
            }
        }
        LOG.debug("Mark file for adding to distributed cache: {0}", actionLibURI.getPath());
        return false;
    }
}
