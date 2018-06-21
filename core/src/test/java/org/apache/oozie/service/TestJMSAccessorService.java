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

package org.apache.oozie.service;

import java.net.URI;
import java.util.Random;

import javax.jms.Session;

import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;
import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.dependency.hcat.HCatMessageHandler;
import org.apache.oozie.jms.ConnectionContext;
import org.apache.oozie.jms.DefaultConnectionContext;
import org.apache.oozie.jms.JMSConnectionInfo;
import org.apache.oozie.jms.MessageReceiver;
import org.apache.oozie.test.XTestCase;
import org.apache.oozie.util.XLog;
import org.junit.Test;

public class TestJMSAccessorService extends XTestCase {
    private static XLog LOG = XLog.getLog(TestJMSAccessorService.class);
    private BrokerService broker;

    private Services services;
    private static Random random = new Random();

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        if (services != null) {
            LOG.debug("services destroy in setUp");
            services.destroy();
        }
        services = super.setupServicesForHCatalog();
        services.init();
    }

    private void waitForConnectionPossible(final JMSConnectionInfo connInfo){
        final JMSAccessorService jmsService = services.get(JMSAccessorService.class);
        LOG.debug("start Waiting until jmsService.createConnectionContext(connInfo) is not null");
        waitFor(5 * 1000, new Predicate() {
            @Override
            public boolean evaluate() throws Exception {
                ConnectionContext connCtxt = jmsService.createConnectionContext(connInfo);
                if (connCtxt == null){
                    return false;
                } else {
                    connCtxt.close();
                    return true;
                }
            }
        });
        LOG.debug("finished Waiting until jmsService.createConnectionContext(connInfo) is not null");
    }

    private void waitForConnected(final JMSConnectionInfo connInfo){
        final JMSAccessorService jmsService = services.get(JMSAccessorService.class);
        LOG.debug("start Waiting until jmsService.isConnectedTo(connInfo)");
        waitFor(5 * 1000, new Predicate() {
            @Override
            public boolean evaluate() throws Exception {
                return (jmsService.isConnectedTo(connInfo));
            }
        });
        LOG.debug("finished Waiting until jmsService.isConnectedTo(connInfo)");
    }

    protected void brokerStart(final String brokerURl, final JMSConnectionInfo connInfo) throws Exception {
        broker = new BrokerService();
        if (brokerURl != null) {
            broker.addConnector(brokerURl);
        } 
        broker.start();
        broker.waitUntilStarted();
        if (connInfo != null) {
            waitForConnected(connInfo);
        }
        LOG.debug("Broker is started:" + broker.isStarted());
    }

    protected void brokerStop() throws Exception {
        if (broker != null) {
            broker.stop();
            broker.waitUntilStopped();
            LOG.debug("Broker is stopped:" + broker.isStopped());
        }
    }

    @Override
    protected void tearDown() throws Exception {
        brokerStop();
        super.tearDown();
    }

    @Test
    public void testConnection() throws Exception {
        HCatAccessorService hcatService = services.get(HCatAccessorService.class);
        JMSAccessorService jmsService = services.get(JMSAccessorService.class);
        // both servers should connect to default JMS server
        JMSConnectionInfo connInfo = hcatService.getJMSConnectionInfo(new URI("hcat://hcatserver.blue.server.com:8020"));
        ConnectionContext ctxt1 = jmsService.createConnectionContext(connInfo);
        assertTrue(ctxt1.isConnectionInitialized());
        JMSConnectionInfo connInfo1 = hcatService.getJMSConnectionInfo(new URI("http://unknown:80"));
        ConnectionContext ctxt2 = jmsService.createConnectionContext(connInfo1);
        assertTrue(ctxt2.isConnectionInitialized());
        assertEquals(ctxt1, ctxt2);
        ctxt1.close();
    }

    @Test
    public void testRegisterSingleConsumerPerTopic() {

        try {
            HCatAccessorService hcatService = services.get(HCatAccessorService.class);
            JMSAccessorService jmsService = services.get(JMSAccessorService.class);
            String server = "hcat.server.com:5080";
            String topic = "hcat.mydb.mytable";

            JMSConnectionInfo connInfo = hcatService.getJMSConnectionInfo(new URI("hcat://hcat.server.com:8020"));
            jmsService.registerForNotification(connInfo, topic, new HCatMessageHandler(server));

            MessageReceiver receiver1 = jmsService.getMessageReceiver(connInfo, topic);
            jmsService.registerForNotification(connInfo, topic, new HCatMessageHandler(server));

            MessageReceiver receiver2 = jmsService.getMessageReceiver(connInfo, topic);
            assertEquals(receiver1, receiver2);
        }
        catch (Exception e) {
            e.printStackTrace();
            fail("Exception encountered : " + e);
        }

    }

    @Test
    public void testUnRegisterTopic() {

        try {
            HCatAccessorService hcatService = services.get(HCatAccessorService.class);
            JMSAccessorService jmsService = services.get(JMSAccessorService.class);
            String server = "hcat.server.com:5080";
            String topic = "hcatalog.mydb.mytable";

            JMSConnectionInfo connInfo = hcatService.getJMSConnectionInfo(new URI("hcat://hcat.server.com:8020"));
            jmsService.registerForNotification(connInfo, topic, new HCatMessageHandler(server));

            MessageReceiver receiver1 = jmsService.getMessageReceiver(connInfo, topic);
            assertNotNull(receiver1);

            jmsService.unregisterFromNotification(connInfo, topic);

            receiver1 = jmsService.getMessageReceiver(connInfo, topic);
            assertEquals(null, receiver1);
        }
        catch (Exception e) {
            e.printStackTrace();
            fail("Exception encountered : " + e);
        }

    }

    @Test
    public void testConnectionContext() {
        try {
            services = super.setupServicesForHCatalog();
            Configuration conf = services.getConf();
            // set the connection factory name
            String jmsURL = "hcat://${1}.${2}.server.com:8020=java.naming.factory.initial#" +
                    "org.apache.activemq.jndi.ActiveMQInitialContextFactory" +
                    ";java.naming.provider.url#vm://localhost?broker.persistent=false;" +
                    "connectionFactoryNames#dynamicFactories/hcat.prod.${1}";
            conf.set(HCatAccessorService.JMS_CONNECTIONS_PROPERTIES, jmsURL);
            services.init();
            HCatAccessorService hcatService = services.get(HCatAccessorService.class);
            JMSConnectionInfo connInfo = hcatService.getJMSConnectionInfo(new URI("hcat://hcatserver.blue.server.com:8020"));
            assertEquals(
                    "java.naming.factory.initial#org.apache.activemq.jndi.ActiveMQInitialContextFactory;java.naming.provider.url#" +
                    "vm://localhost?broker.persistent=false;connectionFactoryNames#dynamicFactories/hcat.prod.hcatserver",
                    connInfo.getJNDIPropertiesString());

            ConnectionContext ctx1 = new DefaultConnectionContext();
            ctx1.createConnection(connInfo.getJNDIProperties());
        }
        catch (Exception e) {
            e.printStackTrace();
            fail("Unexpected exception " + e);
        }
    }

    @Test
    public void testConnectionRetry() throws Exception {
        services = super.setupServicesForHCatalog();
        int randomPort = 30000 + random.nextInt(10000);
        String brokerURl = "tcp://localhost:" + randomPort;
        Configuration servicesConf = services.getConf();
        servicesConf.set(JMSAccessorService.CONF_RETRY_INITIAL_DELAY, "1");
        servicesConf.set(JMSAccessorService.CONF_RETRY_MAX_ATTEMPTS, "3");
        servicesConf.set(HCatAccessorService.JMS_CONNECTIONS_PROPERTIES, "default=java.naming.factory.initial#"
                + ActiveMQConnFactory + ";" + "java.naming.provider.url#" + brokerURl + ";" + "connectionFactoryNames#"
                + "ConnectionFactory");
        services.init();
        HCatAccessorService hcatService = Services.get().get(HCatAccessorService.class);
        JMSAccessorService jmsService = Services.get().get(JMSAccessorService.class);

        String publisherAuthority = "hcat.server.com:5080";
        String topic = "topic.topic1";
        JMSConnectionInfo connInfo = hcatService.getJMSConnectionInfo(new URI("hcat://hcat.server.com:8020"));
        waitForConnectionPossible(connInfo);
        jmsService.registerForNotification(connInfo, topic, new HCatMessageHandler(publisherAuthority));
        assertFalse(jmsService.isListeningToTopic(connInfo, topic));
        assertTrue(jmsService.isConnectionInRetryList(connInfo));
        assertTrue(jmsService.isTopicInRetryList(connInfo, topic));
        // Start the broker and check if listening to topic now
        brokerStart(brokerURl, connInfo);
        assertTrue("jmsService.isListeningToTopic:" + connInfo, jmsService.isListeningToTopic(connInfo, topic));
        assertFalse("jmsService.isConnectionInRetryList:" + connInfo, jmsService.isConnectionInRetryList(connInfo));
        assertFalse("jmsService.isTopicInRetryList:" + connInfo, jmsService.isTopicInRetryList(connInfo, topic));

    }

    @Test
    public void testConnectionRetryExceptionListener() throws Exception {
        services = super.setupServicesForHCatalog();
        int randomPort = 30000 + random.nextInt(10000);
        String brokerURL = "tcp://localhost:" + randomPort;
        String jndiPropertiesString = "java.naming.factory.initial#" + ActiveMQConnFactory + ";"
                + "java.naming.provider.url#" + brokerURL + ";" + "connectionFactoryNames#" + "ConnectionFactory";
        Configuration servicesConf = services.getConf();
        servicesConf.set(JMSAccessorService.CONF_RETRY_INITIAL_DELAY, "1");
        servicesConf.set(JMSAccessorService.CONF_RETRY_MAX_ATTEMPTS, "3");
        servicesConf.set(HCatAccessorService.JMS_CONNECTIONS_PROPERTIES, "default=" + jndiPropertiesString);
        services.init();
        HCatAccessorService hcatService = Services.get().get(HCatAccessorService.class);
        JMSAccessorService jmsService = Services.get().get(JMSAccessorService.class);

        String publisherAuthority = "hcat.server.com:5080";
        String topic = "topic.topic1";
        // Start the broker
        JMSConnectionInfo connInfo = hcatService.getJMSConnectionInfo(new URI("hcat://hcat.server.com:8020"));
        brokerStart(brokerURL, connInfo);
        jmsService.registerForNotification(connInfo, topic, new HCatMessageHandler(publisherAuthority));
        assertTrue(jmsService.isListeningToTopic(connInfo, topic));
        assertFalse(jmsService.isConnectionInRetryList(connInfo));
        assertFalse(jmsService.isTopicInRetryList(connInfo, topic));
        ConnectionContext connCtxt = jmsService.createConnectionContext(connInfo);
        brokerStop();
        try {
            connCtxt.createSession(Session.AUTO_ACKNOWLEDGE);
            fail("Exception expected");
        }
        catch (Exception e) {
            Thread.sleep(100);
            assertFalse(jmsService.isListeningToTopic(connInfo, topic));
            assertTrue(jmsService.isConnectionInRetryList(connInfo));
            assertTrue(jmsService.isTopicInRetryList(connInfo, topic));
        }
        brokerStart(brokerURL, connInfo);

        assertTrue("jms service is listening to topic", jmsService.isListeningToTopic(connInfo, topic));
        assertFalse(jmsService.isConnectionInRetryList(connInfo));
        assertFalse(jmsService.isTopicInRetryList(connInfo, topic));

    }

    @Test
    public void testConnectionRetryMaxAttempt() throws Exception {
        services = super.setupServicesForHCatalog();
        String jndiPropertiesString = "java.naming.factory.initial#" + ActiveMQConnFactory + ";"
                + "java.naming.provider.url#" + "tcp://localhost:12345;connectionFactoryNames#ConnectionFactory";
        Configuration servicesConf = services.getConf();
        Integer retryDelayInSec = 1;
        servicesConf.set(JMSAccessorService.CONF_RETRY_INITIAL_DELAY, retryDelayInSec.toString());
        servicesConf.set(JMSAccessorService.CONF_RETRY_MAX_ATTEMPTS, "1");
        servicesConf.set(HCatAccessorService.JMS_CONNECTIONS_PROPERTIES, "default=" + jndiPropertiesString);
        services.init();
        HCatAccessorService hcatService = Services.get().get(HCatAccessorService.class);
        JMSAccessorService jmsService = Services.get().get(JMSAccessorService.class);

        String publisherAuthority = "hcat.server.com:5080";
        String topic = "topic.topic1";
        JMSConnectionInfo connInfo = hcatService.getJMSConnectionInfo(new URI("hcat://hcat.server.com:8020"));
        jmsService.registerForNotification(connInfo, topic, new HCatMessageHandler(publisherAuthority));
        assertTrue(jmsService.isConnectionInRetryList(connInfo));
        assertTrue(jmsService.isTopicInRetryList(connInfo, topic));
        assertFalse(jmsService.isListeningToTopic(connInfo, topic));
        Thread.sleep((retryDelayInSec * 1000) + 100);
        // Should not retry again as max attempt is 1
        assertTrue(jmsService.isConnectionInRetryList(connInfo));
        assertTrue(jmsService.isTopicInRetryList(connInfo, topic));
        assertFalse(jmsService.isListeningToTopic(connInfo, topic));
        assertEquals(1, jmsService.getNumConnectionAttempts(connInfo));
        assertFalse(jmsService.retryConnection(connInfo));
    }

}
