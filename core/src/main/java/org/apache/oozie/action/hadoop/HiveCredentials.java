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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.security.token.Token;
import org.apache.hive.jdbc.HiveConnection;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.action.ActionExecutor;
import org.apache.oozie.util.XLog;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.HashMap;


public class HiveCredentials extends Credentials{

    private static final String USER_NAME = "user.name";
    private static final String BEELINE_HIVE2_JDBC_URL = "beeline.hs2.jdbc.url.container";
    private static final String HIVE2_SERVER_PRINCIPAL = "hive2.server.principal";
    private static final String HIVE_JDBC_URL = "hive.jdbc.url";
    private static final String HIVE_PRINCIPAL = "hive.principal";
    private static final String HCAT_METASTORE_PRINCIPAL = "hcat.metastore.principal";
    private static final String HIVE_METASTORE_PRINCIPAL = "hive.metastore.kerberos.principal";
    private static final String HIVE_CONF_LOCATION = "/etc/hive/conf/";
    private static final String HIVE_SITE = "hive-site.xml";
    private static final String BEELINE_SITE = "beeline-site.xml";
    private final static Configuration hiveConf = new Configuration(false);
    private final static Configuration beelineConf = new Configuration(false);
    static {
        hiveConf.addResource(HIVE_SITE);
        beelineConf.addResource(HIVE_CONF_LOCATION + BEELINE_SITE);
    }

    @Override
    public void addtoJobConf(JobConf jobconf, CredentialsProperties props, ActionExecutor.Context context) throws Exception {
        try {
            // load the driver
            Class.forName("org.apache.hive.jdbc.HiveDriver");

            String url = jobconf.get(BEELINE_HIVE2_JDBC_URL);
            if (url == null || url.isEmpty()) {
                throw new CredentialException(ErrorCode.E0510,
                        BEELINE_HIVE2_JDBC_URL + " is required to get hive server 2 credential");
            }

            String principal = getProperty(props.getProperties(), HCAT_METASTORE_PRINCIPAL, HIVE_METASTORE_PRINCIPAL);
            XLog.getLog(getClass()).debug(HIVE_METASTORE_PRINCIPAL+"=" +  principal);

            if (principal == null || principal.isEmpty()) {
                XLog.getLog(getClass()).debug(HCAT_METASTORE_PRINCIPAL+"=" +  principal);
                throw new CredentialException(ErrorCode.E0510,
                        HCAT_METASTORE_PRINCIPAL + " or " + HIVE2_SERVER_PRINCIPAL + " is required to get hive server 2 credential");
            }

            String urlWithPrincipal =url + ";principal=" + principal;
            Connection con = null;
            String tokenStr = null;
            try {
                con = DriverManager.getConnection(urlWithPrincipal);
                XLog.getLog(getClass()).debug("Connected successfully to " + url);
                // get delegation token for the given proxy user
                tokenStr = ((HiveConnection)con).getDelegationToken(jobconf.get(USER_NAME), principal);
            } finally {
                if (con != null) {
                    con.close();
                }
            }
            XLog.getLog(getClass()).debug("Got token");

            Token<DelegationTokenIdentifier> hive2Token = new Token<DelegationTokenIdentifier>();
            hive2Token.decodeFromUrlString(tokenStr);
            jobconf.getCredentials().addToken(new Text("hive.server2.delegation.token"), hive2Token);
            jobconf.set(HIVE_JDBC_URL, url);
            jobconf.set(HIVE_PRINCIPAL, principal);

            XLog.getLog(getClass()).debug("Added the Hive Server 2 token in job conf");
        }
        catch (Exception e) {
            XLog.getLog(getClass()).warn("Exception in addtoJobConf", e);
            throw e;
        }
    }

    /**
     * Returns the value for the oozieConfName if its present in prop map else
     * value of hiveConfName.
     *
     * @param prop
     * @param oozieConfName
     * @param hiveConfName
     * @return value for the oozieConfName if its present else value of
     *         hiveConfName. If both are absent then returns null.
     */
    private String getProperty(HashMap<String, String> prop, String oozieConfName, String hiveConfName) {
        String value = prop.get(oozieConfName) == null ? prop.get(hiveConfName) : prop.get(oozieConfName);

        // user provided hive-site.xml
        if (value == null || value.isEmpty()) {
            value = hiveConf.get(hiveConfName);
        }

        // /etc/hive/conf/hive-site.xml
        if (value == null || value.isEmpty()) {
            value = getConf(HIVE_CONF_LOCATION + HIVE_SITE).get(hiveConfName);
        }
        return value;
    }

    private Configuration getConf(String configLocation) {
        Configuration config = new Configuration();
        config.addResource(configLocation);
        return config;
    }
}
