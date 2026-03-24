/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.activemq.artemis.tests.integration.amqp.connect;

import static org.apache.activemq.artemis.protocol.amqp.proton.AMQPArtemisMessageFormats.AMQP_COMPRESSED_LARGE_MESSAGE_FORMAT;
import static org.apache.activemq.artemis.protocol.amqp.proton.AMQPArtemisMessageFormats.AMQP_COMPRESSED_MESSAGE_FORMAT;
import static org.apache.activemq.artemis.protocol.amqp.proton.AMQPArtemisMessageFormats.AMQP_COMPRESSED_TUNNELED_CORE_MESSAGE_FORMAT;
import static org.apache.activemq.artemis.protocol.amqp.proton.AMQPArtemisMessageFormats.AMQP_TUNNELED_CORE_MESSAGE_FORMAT;
import static org.apache.activemq.artemis.protocol.amqp.connect.federation.AMQPFederationConstants.FEDERATION_CONFIGURATION;
import static org.apache.activemq.artemis.protocol.amqp.connect.federation.AMQPFederationConstants.FEDERATION_CONTROL_LINK;
import static org.apache.activemq.artemis.protocol.amqp.connect.federation.AMQPFederationConstants.FEDERATION_EVENT_LINK;
import static org.apache.activemq.artemis.protocol.amqp.connect.federation.AMQPFederationConstants.FEDERATION_NAME;
import static org.apache.activemq.artemis.protocol.amqp.connect.federation.AMQPFederationConstants.FEDERATION_ADDRESS_RECEIVER;
import static org.apache.activemq.artemis.protocol.amqp.connect.federation.AMQPFederationConstants.FEDERATION_QUEUE_RECEIVER;
import static org.apache.activemq.artemis.protocol.amqp.connect.federation.AMQPFederationConstants.RECEIVER_CREDITS;
import static org.apache.activemq.artemis.protocol.amqp.connect.federation.AMQPFederationConstants.RECEIVER_CREDITS_LOW;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.MessageProducer;
import javax.jms.Session;

import org.apache.activemq.artemis.api.core.QueueConfiguration;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.protocol.amqp.proton.AmqpSupport;
import org.apache.activemq.artemis.tests.integration.amqp.AmqpClientTestSupport;
import org.apache.activemq.artemis.tests.util.CFUtil;
import org.apache.qpid.protonj2.test.driver.ProtonTestClient;
import org.apache.qpid.protonj2.test.driver.codec.primitives.Binary;
import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;
import org.jgroups.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Tests for AMQP Broker federation handling of inflight message compression.
 */
public class AMQPFederationMessageCompressionTest extends AmqpClientTestSupport {

   private static final int MIN_LARGE_MESSAGE_SIZE = 10 * 1024;
   private static final int MAX_FRAME_SIZE_DEFAULT = 128 * 1024;

   @Override
   protected String getConfiguredProtocols() {
      return "AMQP,CORE";
   }

   @Override
   protected ActiveMQServer createServer() throws Exception {
      // Creates the broker used to make the outgoing connection. The port passed is for
      // that brokers acceptor. The test server connected to by the broker binds to a random port.
      return createServer(AMQP_PORT, false);
   }

   @Test
   @Timeout(20)
   public void testAddressFederationAMQPMessageCompressedWhenInflightCompressionEnabled() throws Exception {
      doTestAddressFederationAMQPMessageCompressionHandlingBasedOnConfiguration(true);
   }

   @Test
   @Timeout(20)
   public void testAddressFederationAMQPMessageNotCompressedWhenInflightCompressionDisabled() throws Exception {
      doTestAddressFederationAMQPMessageCompressionHandlingBasedOnConfiguration(false);
   }

   private void doTestAddressFederationAMQPMessageCompressionHandlingBasedOnConfiguration(boolean compress) throws Exception {
      server.start();

      final String[] receiverOfferedCapabilities;
      final int messageFormat;
      final String payload = "A".repeat(1024);

      if (compress) {
         receiverOfferedCapabilities = new String[] {AmqpSupport.INFLIGHT_MESSAGE_COMPRESSION_SUPPORT.toString()};
         messageFormat = AMQP_COMPRESSED_MESSAGE_FORMAT;
      } else {
         receiverOfferedCapabilities = null;
         messageFormat = 0;
      }

      try (ProtonTestClient peer = new ProtonTestClient()) {
         scriptFederationConnectToRemote(peer, "test");
         peer.connect("localhost", AMQP_PORT);

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         peer.expectAttach().ofSender().withName("federation-address-receiver")
                                       .withOfferedCapabilities(FEDERATION_ADDRESS_RECEIVER.toString())
                                       .withDesiredCapabilities(AmqpSupport.CORE_MESSAGE_TUNNELING_SUPPORT.toString(),
                                                                AmqpSupport.INFLIGHT_MESSAGE_COMPRESSION_SUPPORT.toString())
                                       .withSource().withAddress(getTestName());

         // Connect to remote as if an address had demand and matched our federation policy
         // If inflight message compression is enabled we include the desired capability
         peer.remoteAttach().ofReceiver()
                            .withOfferedCapabilities(receiverOfferedCapabilities)
                            .withDesiredCapabilities(FEDERATION_ADDRESS_RECEIVER.toString())
                            .withName("federation-address-receiver")
                            .withSenderSettleModeUnsettled()
                            .withReceivervSettlesFirst()
                            .withSource().withDurabilityOfNone()
                                         .withExpiryPolicyOnLinkDetach()
                                         .withAddress(getTestName())
                                         .withCapabilities("topic")
                                         .and()
                            .withTarget().and()
                            .now();
         peer.remoteFlow().withLinkCredit(10).now();
         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);

         if (compress) {
            peer.expectTransfer().withMessage().withData(new DataSectionSizeLessThanMatcher(payload.length()))
                                 .withMessageFormat(messageFormat)
                                 .and()
                                 .accept();
         } else {
            peer.expectTransfer().withMessageFormat(messageFormat)
                                 .withMessage()
                                 .withHeader().and()
                                 .withProperties().and()
                                 .withMessageAnnotations().and()
                                 .withValue(payload).and()
                                 .accept();
         }

         final ConnectionFactory factory = CFUtil.createConnectionFactory("AMQP", "tcp://localhost:" + AMQP_PORT);

         try (Connection connection = factory.createConnection()) {
            final Session session = connection.createSession(Session.AUTO_ACKNOWLEDGE);
            final MessageProducer producer = session.createProducer(session.createTopic(getTestName()));

            producer.send(session.createTextMessage(payload));

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         }

         peer.expectClose();
         peer.remoteClose().now();

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         peer.close();

         server.stop();
      }
   }

   @Test
   @Timeout(20)
   public void testAddressFederationAMQPLargeMessageCompressedWhenInflightCompressionEnabled() throws Exception {
      doTestAddressFederationAMQPLargeMessageCompressionHandlingBasedOnConfiguration(true);
   }

   @Test
   @Timeout(20)
   public void testAddressFederationAMQPLargeMessageNotCompressedWhenInflightCompressionDisabled() throws Exception {
      doTestAddressFederationAMQPLargeMessageCompressionHandlingBasedOnConfiguration(false);
   }

   private void doTestAddressFederationAMQPLargeMessageCompressionHandlingBasedOnConfiguration(boolean compress) throws Exception {
      server.start();

      // Double check that the message body should fit within one frame for this test.
      assertTrue(MIN_LARGE_MESSAGE_SIZE < MAX_FRAME_SIZE_DEFAULT / 2);

      final String[] receiverOfferedCapabilities;
      final int messageFormat;
      final String payload = "AA".repeat(MIN_LARGE_MESSAGE_SIZE);

      if (compress) {
         receiverOfferedCapabilities = new String[] {AmqpSupport.INFLIGHT_MESSAGE_COMPRESSION_SUPPORT.toString()};
         messageFormat = AMQP_COMPRESSED_LARGE_MESSAGE_FORMAT;
      } else {
         receiverOfferedCapabilities = null;
         messageFormat = 0;
      }

      try (ProtonTestClient peer = new ProtonTestClient()) {
         scriptFederationConnectToRemote(peer, "test");
         peer.connect("localhost", AMQP_PORT);

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         peer.expectAttach().ofSender().withName("federation-address-receiver")
                                       .withOfferedCapabilities(FEDERATION_ADDRESS_RECEIVER.toString())
                                       .withDesiredCapabilities(AmqpSupport.CORE_MESSAGE_TUNNELING_SUPPORT.toString(),
                                                                AmqpSupport.INFLIGHT_MESSAGE_COMPRESSION_SUPPORT.toString())
                                       .withSource().withAddress(getTestName());

         // Connect to remote as if an address had demand and matched our federation policy
         // If inflight message compression is enabled we include the desired capability
         peer.remoteAttach().ofReceiver()
                            .withOfferedCapabilities(receiverOfferedCapabilities)
                            .withDesiredCapabilities(FEDERATION_ADDRESS_RECEIVER.toString())
                            .withName("federation-address-receiver")
                            .withSenderSettleModeUnsettled()
                            .withReceivervSettlesFirst()
                            .withSource().withDurabilityOfNone()
                                         .withExpiryPolicyOnLinkDetach()
                                         .withAddress(getTestName())
                                         .withCapabilities("topic")
                                         .and()
                            .withTarget().and()
                            .now();
         peer.remoteFlow().withLinkCredit(10).now();
         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);

         if (compress) {
            // First transfer is Header and Delivery annotations
            peer.expectTransfer().withMessageFormat(messageFormat)
                                 .withMessage()
                                 .withHeader().also()
                                 .withData(new DataSectionSizeLessThanMatcher(payload.length()))
                                 .and()
                                 .withMore(true);
            // This is compressed AMQP message minus any delivery annotations.
            peer.expectTransfer().withMessage().withData(new DataSectionSizeLessThanMatcher(payload.length()))
                                 .withMessageFormat(messageFormat)
                                 .and()
                                 .withMore(true);
            // Final transfer is emitted once the sender is advanced
            peer.expectTransfer().withMessageFormat(messageFormat)
                                 .withNullPayload()
                                 .withMore(nullValue())
                                 .accept();
         } else {
            // Typical Qpid JMS AMQP message layout
            peer.expectTransfer().withMessageFormat(messageFormat)
                                 .withMessage()
                                 .withHeader().and()
                                 .withProperties().and()
                                 .withMessageAnnotations().and()
                                 .withValue(payload).and()
                                 .withMore(true)
                                 .accept();
            // Final transfer is emitted once the sender is advanced
            peer.expectTransfer().withMessageFormat(messageFormat)
                                 .withNullPayload()
                                 .withMore(nullValue())
                                 .accept();
         }

         final ConnectionFactory factory = CFUtil.createConnectionFactory("AMQP", "tcp://localhost:" + AMQP_PORT);

         try (Connection connection = factory.createConnection()) {
            final Session session = connection.createSession(Session.AUTO_ACKNOWLEDGE);
            final MessageProducer producer = session.createProducer(session.createTopic(getTestName()));

            producer.send(session.createTextMessage(payload));

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         }

         peer.expectClose();
         peer.remoteClose().now();

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         peer.close();

         server.stop();
      }
   }

   @Test
   @Timeout(20)
   public void testAddressFederationCoreTunneledMessageCompressedWhenInflightCompressionEnabled() throws Exception {
      doTestAddressFederationCoreTunneledMessageCompressionHandlingBasedOnConfiguration(true);
   }

   @Test
   @Timeout(20)
   public void testAddressFederationCoreTunneledMessageNotCompressedWhenInflightCompressionDisabled() throws Exception {
      doTestAddressFederationCoreTunneledMessageCompressionHandlingBasedOnConfiguration(false);
   }

   private void doTestAddressFederationCoreTunneledMessageCompressionHandlingBasedOnConfiguration(boolean compress) throws Exception {
      server.start();

      final String[] receiverOfferedCapabilities;
      final int messageFormat;
      final String payload = "A".repeat(1024);

      if (compress) {
         receiverOfferedCapabilities = new String[] {AmqpSupport.CORE_MESSAGE_TUNNELING_SUPPORT.toString(),
                                                     AmqpSupport.INFLIGHT_MESSAGE_COMPRESSION_SUPPORT.toString()};
         messageFormat = AMQP_COMPRESSED_TUNNELED_CORE_MESSAGE_FORMAT;
      } else {
         receiverOfferedCapabilities = new String[] {AmqpSupport.CORE_MESSAGE_TUNNELING_SUPPORT.toString()};
         messageFormat = AMQP_TUNNELED_CORE_MESSAGE_FORMAT;
      }

      try (ProtonTestClient peer = new ProtonTestClient()) {
         scriptFederationConnectToRemote(peer, "test");
         peer.connect("localhost", AMQP_PORT);

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         peer.expectAttach().ofSender().withName("federation-address-receiver")
                                       .withOfferedCapabilities(FEDERATION_ADDRESS_RECEIVER.toString())
                                       .withDesiredCapabilities(AmqpSupport.CORE_MESSAGE_TUNNELING_SUPPORT.toString(),
                                                                AmqpSupport.INFLIGHT_MESSAGE_COMPRESSION_SUPPORT.toString())
                                       .withSource().withAddress(getTestName());

         // Connect to remote as if an address had demand and matched our federation policy
         // If inflight message compression is enabled we include the desired capability
         peer.remoteAttach().ofReceiver()
                            .withOfferedCapabilities(receiverOfferedCapabilities)
                            .withDesiredCapabilities(FEDERATION_ADDRESS_RECEIVER.toString())
                            .withName("federation-address-receiver")
                            .withSenderSettleModeUnsettled()
                            .withReceivervSettlesFirst()
                            .withSource().withDurabilityOfNone()
                                         .withExpiryPolicyOnLinkDetach()
                                         .withAddress(getTestName())
                                         .withCapabilities("topic")
                                         .and()
                            .withTarget().and()
                            .now();
         peer.remoteFlow().withLinkCredit(10).now();
         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);

         if (compress) {
            peer.expectTransfer().withMessage().withData(new DataSectionSizeLessThanMatcher(payload.length()))
                                 .withMessageFormat(messageFormat)
                                 .and()
                                 .accept();
         } else {
            peer.expectTransfer().withMessageFormat(messageFormat).accept();
         }

         final ConnectionFactory factory = CFUtil.createConnectionFactory("CORE", "tcp://localhost:" + AMQP_PORT);

         try (Connection connection = factory.createConnection()) {
            final Session session = connection.createSession(Session.AUTO_ACKNOWLEDGE);
            final MessageProducer producer = session.createProducer(session.createTopic(getTestName()));

            producer.send(session.createTextMessage(payload));

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         }

         peer.expectClose();
         peer.remoteClose().now();

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         peer.close();

         server.stop();
      }
   }

   @Test
   @Timeout(20)
   public void testQueueFederationAMQPMessageCompressedWhenInflightCompressionEnabled() throws Exception {
      doTestQueueFederationAMQPMessageCompressionHandlingBasedOnConfiguration(true);
   }

   @Test
   @Timeout(20)
   public void testQueueFederationAMQPMessageNotCompressedWhenInflightCompressionDisabled() throws Exception {
      doTestQueueFederationAMQPMessageCompressionHandlingBasedOnConfiguration(false);
   }

   private void doTestQueueFederationAMQPMessageCompressionHandlingBasedOnConfiguration(boolean compress) throws Exception {
      server.start();
      server.createQueue(QueueConfiguration.of(getTestName()).setRoutingType(RoutingType.ANYCAST)
                                                             .setAddress(getTestName())
                                                             .setAutoCreated(false));

      final String[] receiverOfferedCapabilities;
      final int messageFormat;
      final String payload = "A".repeat(1024);

      if (compress) {
         receiverOfferedCapabilities = new String[] {AmqpSupport.INFLIGHT_MESSAGE_COMPRESSION_SUPPORT.toString()};
         messageFormat = AMQP_COMPRESSED_MESSAGE_FORMAT;
      } else {
         receiverOfferedCapabilities = null;
         messageFormat = 0;
      }

      try (ProtonTestClient peer = new ProtonTestClient()) {
         scriptFederationConnectToRemote(peer, "test");
         peer.connect("localhost", AMQP_PORT);

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         peer.expectAttach().ofSender().withName("federation-queue-receiver")
                                       .withOfferedCapabilities(FEDERATION_QUEUE_RECEIVER.toString())
                                       .withDesiredCapabilities(AmqpSupport.CORE_MESSAGE_TUNNELING_SUPPORT.toString(),
                                                                AmqpSupport.INFLIGHT_MESSAGE_COMPRESSION_SUPPORT.toString())
                                       .withSource().withAddress(getTestName() + "::" + getTestName());

         // Connect to remote as if an queue had demand and matched our federation policy
         // If inflight message compression is enabled we include the desired capability
         peer.remoteAttach().ofReceiver()
                            .withOfferedCapabilities(receiverOfferedCapabilities)
                            .withDesiredCapabilities(FEDERATION_QUEUE_RECEIVER.toString())
                            .withName("federation-queue-receiver")
                            .withSenderSettleModeUnsettled()
                            .withReceivervSettlesFirst()
                            .withSource().withDurabilityOfNone()
                                         .withExpiryPolicyOnLinkDetach()
                                         .withAddress(getTestName() + "::" + getTestName())
                                         .withCapabilities("queue")
                                         .and()
                            .withTarget().and()
                            .now();
         peer.remoteFlow().withLinkCredit(10).now();
         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);

         if (compress) {
            peer.expectTransfer().withMessage().withData(new DataSectionSizeLessThanMatcher(payload.length()))
                                 .withMessageFormat(messageFormat)
                                 .and()
                                 .accept();
         } else {
            peer.expectTransfer().withMessageFormat(messageFormat)
                                 .withMessage()
                                 .withHeader().and()
                                 .withProperties().and()
                                 .withMessageAnnotations().and()
                                 .withValue(payload).and()
                                 .accept();
         }

         final ConnectionFactory factory = CFUtil.createConnectionFactory("AMQP", "tcp://localhost:" + AMQP_PORT);

         try (Connection connection = factory.createConnection()) {
            final Session session = connection.createSession(Session.AUTO_ACKNOWLEDGE);
            final MessageProducer producer = session.createProducer(session.createQueue(getTestName()));

            producer.send(session.createTextMessage(payload));

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         }

         peer.expectClose();
         peer.remoteClose().now();

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         peer.close();

         server.stop();
      }
   }

   @Test
   @Timeout(20)
   public void testQueueFederationCoreTunneledMessageCompressedWhenInflightCompressionEnabled() throws Exception {
      doTestQueueFederationCoreTunneledMessageCompressionHandlingBasedOnConfiguration(true);
   }

   @Test
   @Timeout(20)
   public void testQueueFederationCoreTunneledMessageNotCompressedWhenInflightCompressionDisabled() throws Exception {
      doTestQueueFederationCoreTunneledMessageCompressionHandlingBasedOnConfiguration(false);
   }

   private void doTestQueueFederationCoreTunneledMessageCompressionHandlingBasedOnConfiguration(boolean compress) throws Exception {
      server.start();
      server.createQueue(QueueConfiguration.of(getTestName()).setRoutingType(RoutingType.ANYCAST)
                                                             .setAddress(getTestName())
                                                             .setAutoCreated(false));

      final String[] receiverOfferedCapabilities;
      final int messageFormat;
      final String payload = "A".repeat(1024);

      if (compress) {
         receiverOfferedCapabilities = new String[] {AmqpSupport.CORE_MESSAGE_TUNNELING_SUPPORT.toString(),
                                                     AmqpSupport.INFLIGHT_MESSAGE_COMPRESSION_SUPPORT.toString()};
         messageFormat = AMQP_COMPRESSED_TUNNELED_CORE_MESSAGE_FORMAT;
      } else {
         receiverOfferedCapabilities = new String[] {AmqpSupport.CORE_MESSAGE_TUNNELING_SUPPORT.toString()};
         messageFormat = AMQP_TUNNELED_CORE_MESSAGE_FORMAT;
      }

      try (ProtonTestClient peer = new ProtonTestClient()) {
         scriptFederationConnectToRemote(peer, "test");
         peer.connect("localhost", AMQP_PORT);

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         peer.expectAttach().ofSender().withName("federation-queue-receiver")
                                       .withOfferedCapabilities(FEDERATION_QUEUE_RECEIVER.toString())
                                       .withDesiredCapabilities(AmqpSupport.CORE_MESSAGE_TUNNELING_SUPPORT.toString(),
                                                                AmqpSupport.INFLIGHT_MESSAGE_COMPRESSION_SUPPORT.toString())
                                       .withSource().withAddress(getTestName() + "::" + getTestName());

         // Connect to remote as if an queue had demand and matched our federation policy
         // If inflight message compression is enabled we include the desired capability
         peer.remoteAttach().ofReceiver()
                            .withOfferedCapabilities(receiverOfferedCapabilities)
                            .withDesiredCapabilities(FEDERATION_QUEUE_RECEIVER.toString())
                            .withName("federation-queue-receiver")
                            .withSenderSettleModeUnsettled()
                            .withReceivervSettlesFirst()
                            .withSource().withDurabilityOfNone()
                                         .withExpiryPolicyOnLinkDetach()
                                         .withAddress(getTestName() + "::" + getTestName())
                                         .withCapabilities("queue")
                                         .and()
                            .withTarget().and()
                            .now();
         peer.remoteFlow().withLinkCredit(10).now();
         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);

         if (compress) {
            peer.expectTransfer().withMessage().withData(new DataSectionSizeLessThanMatcher(payload.length()))
                                 .withMessageFormat(messageFormat)
                                 .and()
                                 .accept();
         } else {
            peer.expectTransfer().withMessageFormat(messageFormat).accept();
         }

         final ConnectionFactory factory = CFUtil.createConnectionFactory("CORE", "tcp://localhost:" + AMQP_PORT);

         try (Connection connection = factory.createConnection()) {
            final Session session = connection.createSession(Session.AUTO_ACKNOWLEDGE);
            final MessageProducer producer = session.createProducer(session.createQueue(getTestName()));

            producer.send(session.createTextMessage(payload));

            peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         }

         peer.expectClose();
         peer.remoteClose().now();

         peer.waitForScriptToComplete(5, TimeUnit.SECONDS);
         peer.close();

         server.stop();
      }
   }

   private static class DataSectionSizeLessThanMatcher extends TypeSafeMatcher<Binary> {

      private final int maxSize;

      private int actualSize;

      DataSectionSizeLessThanMatcher(int maxSize) {
         this.maxSize = maxSize;
      }

      @Override
      public void describeTo(Description description) {
         description.appendText("Expected data section with size less than: ")
                    .appendValue(maxSize)
                    .appendText(", but got a section with size: ")
                    .appendValue(actualSize);
      }

      @Override
      protected boolean matchesSafely(Binary payload) {
         if (payload == null || payload.getLength() == 0) {
            return false; // We expect some size if using this matcher
         }

         if (payload.getLength() > maxSize) {
            return false;
         }

         return true;
      }
   }

   // Use this method to script the initial handshake that a broker that is establishing
   // a federation connection with a remote broker instance would perform.
   private void scriptFederationConnectToRemote(ProtonTestClient peer, String federationName) {
      scriptFederationConnectToRemote(peer, federationName, AmqpSupport.AMQP_CREDITS_DEFAULT, AmqpSupport.AMQP_LOW_CREDITS_DEFAULT);
   }

   private void scriptFederationConnectToRemote(ProtonTestClient peer, String federationName, int amqpCredits, int amqpLowCredits) {
      scriptFederationConnectToRemote(peer, federationName, amqpCredits, amqpLowCredits, false, false);
   }

   private void scriptFederationConnectToRemote(ProtonTestClient peer, String federationName, int amqpCredits, int amqpLowCredits, boolean eventsSender, boolean eventsReceiver) {
      scriptFederationConnectToRemote(peer, null, null, federationName, amqpCredits, amqpLowCredits, eventsSender, eventsReceiver);
   }

   private void scriptFederationConnectToRemote(ProtonTestClient peer, String user, String password, String federationName, int amqpCredits, int amqpLowCredits, boolean eventsSender, boolean eventsReceiver) {

      final String federationControlLinkName = "Federation:control:" + UUID.randomUUID().toString();

      final Map<String, Object> federationConfiguration = new HashMap<>();
      federationConfiguration.put(RECEIVER_CREDITS, amqpCredits);
      federationConfiguration.put(RECEIVER_CREDITS_LOW, amqpLowCredits);

      final Map<String, Object> senderProperties = new HashMap<>();
      senderProperties.put(FEDERATION_CONFIGURATION.toString(), federationConfiguration);
      senderProperties.put(FEDERATION_NAME.toString(), federationName);

      if (user == null && password == null) {
         peer.queueClientSaslAnonymousConnect();
      } else {
         peer.queueClientSaslPlainConnect(user, password);
      }

      peer.remoteOpen().queue();
      peer.expectOpen();
      peer.remoteBegin().queue();
      peer.expectBegin();
      peer.remoteAttach().ofSender()
                         .withInitialDeliveryCount(0)
                         .withName(federationControlLinkName)
                         .withPropertiesMap(senderProperties)
                         .withDesiredCapabilities(FEDERATION_CONTROL_LINK.toString())
                         .withSenderSettleModeUnsettled()
                         .withReceivervSettlesFirst()
                         .withSource().also()
                         .withTarget().withDynamic(true)
                                      .withDurabilityOfNone()
                                      .withExpiryPolicyOnLinkDetach()
                                      .withLifetimePolicyOfDeleteOnClose()
                                      .withCapabilities("temporary-topic")
                                      .also()
                         .queue();
      peer.expectAttach().ofReceiver()
                         .withTarget()
                            .withAddress(notNullValue())
                         .also()
                         .withOfferedCapability(FEDERATION_CONTROL_LINK.toString());
      peer.expectFlow();

      // Sender created when there are remote policies to send to the target
      if (eventsSender) {
         final String federationEventsSenderLinkName = "Federation:events-sender:test:" + UUID.randomUUID().toString();

         peer.remoteAttach().ofSender()
                            .withName(federationEventsSenderLinkName)
                            .withDesiredCapabilities(FEDERATION_EVENT_LINK.toString())
                            .withSenderSettleModeSettled()
                            .withReceivervSettlesFirst()
                            .withSource().also()
                            .withTarget().withDynamic(true)
                                         .withDurabilityOfNone()
                                         .withExpiryPolicyOnLinkDetach()
                                         .withLifetimePolicyOfDeleteOnClose()
                                         .withCapabilities("temporary-topic")
                                         .also()
                                         .queue();
         peer.expectAttach().ofReceiver()
                            .withName(federationEventsSenderLinkName)
                            .withTarget()
                               .withAddress(notNullValue())
                            .also()
                            .withOfferedCapability(FEDERATION_EVENT_LINK.toString());
         peer.expectFlow();
      }

      // Receiver created when there are local policies on the source.
      if (eventsReceiver) {
         final String federationEventsSenderLinkName = "Federation:events-receiver:test:" + UUID.randomUUID().toString();

         peer.remoteAttach().ofReceiver()
                            .withName(federationEventsSenderLinkName)
                            .withDesiredCapabilities(FEDERATION_EVENT_LINK.toString())
                            .withSenderSettleModeSettled()
                            .withReceivervSettlesFirst()
                            .withTarget().also()
                            .withSource().withDynamic(true)
                                         .withDurabilityOfNone()
                                         .withExpiryPolicyOnLinkDetach()
                                         .withLifetimePolicyOfDeleteOnClose()
                                         .withCapabilities("temporary-topic")
                                         .also()
                                         .queue();
         peer.remoteFlow().withLinkCredit(10).queue();
         peer.expectAttach().ofSender()
                            .withName(federationEventsSenderLinkName)
                            .withSource()
                               .withAddress(notNullValue())
                            .also()
                            .withOfferedCapability(FEDERATION_EVENT_LINK.toString());
      }
   }
}
