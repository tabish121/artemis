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

package org.apache.activemq.artemis.protocol.amqp.proton;

import java.lang.invoke.MethodHandles;
import java.nio.ByteBuffer;
import java.util.zip.Inflater;

import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.api.core.ActiveMQBuffers;
import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.core.server.ActiveMQServerLogger;
import org.apache.activemq.artemis.protocol.amqp.broker.AMQPLargeMessage;
import org.apache.activemq.artemis.protocol.amqp.broker.AMQPSessionCallback;
import org.apache.activemq.artemis.protocol.amqp.exceptions.ActiveMQAMQPInternalErrorException;
import org.apache.activemq.artemis.protocol.amqp.util.TLSEncode;
import org.apache.qpid.proton.amqp.messaging.AmqpSequence;
import org.apache.qpid.proton.amqp.messaging.AmqpValue;
import org.apache.qpid.proton.amqp.messaging.ApplicationProperties;
import org.apache.qpid.proton.amqp.messaging.Data;
import org.apache.qpid.proton.amqp.messaging.DeliveryAnnotations;
import org.apache.qpid.proton.amqp.messaging.Footer;
import org.apache.qpid.proton.amqp.messaging.Header;
import org.apache.qpid.proton.amqp.messaging.MessageAnnotations;
import org.apache.qpid.proton.amqp.messaging.Properties;
import org.apache.qpid.proton.codec.CompositeReadableBuffer;
import org.apache.qpid.proton.codec.DecoderImpl;
import org.apache.qpid.proton.codec.EncodingCodes;
import org.apache.qpid.proton.codec.ReadableBuffer;
import org.apache.qpid.proton.codec.TypeConstructor;
import org.apache.qpid.proton.engine.Delivery;
import org.apache.qpid.proton.engine.Receiver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reader of {@link AMQPLargeMessage} content which reads and inflates all bytes and completes once a non-partial
 * delivery is read.
 */
public class AMQPLargeMessageInflatingReader implements MessageReader {

   private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

   private enum State {
      /**
       * Awaiting initial decode of first sections in delivery which could be a header or delivery annotations
       * or could be the first data section which will be part of the compressed AMQP message.
       */
      INITIALIZING,
      /**
       * Accumulating the bytes needed to identify the next incoming Data section containing compressed bytes.
       */
      BODY_SECTION_PENDING,
      /**
       * Accumulating the message payload from the incoming Data sections that comprise the compressed AMQP message.
       */
      BODY_INFLATING,
      /**
       * The full message has been read and no more incoming bytes are accepted.
       */
      DONE,
      /**
       * Indicates the reader is closed and cannot be used until opened.
       */
      CLOSED
   }

   private static final int SCRATCH_BUFFER_SIZE = 2048;

   private final ProtonAbstractReceiver serverReceiver;
   private final CompositeReadableBuffer pendingRecvBuffer = new CompositeReadableBuffer();
   private final Inflater inflater = new Inflater();
   private final byte[] inflateFrom = new byte[SCRATCH_BUFFER_SIZE];
   private final ByteBuffer inflateTo = ByteBuffer.allocate(SCRATCH_BUFFER_SIZE);

   private volatile AMQPLargeMessage currentMessage;
   private volatile State state = State.CLOSED;
   private int dataSectionRemaining;
   private DeliveryAnnotations deliveryAnnotations;

   public AMQPLargeMessageInflatingReader(ProtonAbstractReceiver serverReceiver) {
      this.serverReceiver = serverReceiver;
   }

   @Override
   public DeliveryAnnotations getDeliveryAnnotations() {
      return deliveryAnnotations;
   }

   @Override
   public void close() {
      if (state != State.CLOSED) {
         try {
            final AMQPSessionCallback sessionSPI = serverReceiver.getSessionContext().getSessionSPI();

            if (currentMessage != null) {
               sessionSPI.execute(() -> {
                  // Run the file delete on the session thread, this allows processing of the
                  // last addBytes to complete which might allow the message to be fully read
                  // in which case currentMessage will be nulled and we won't delete it as it
                  // will have already been handed to the connection thread for enqueue.
                  if (currentMessage != null) {
                     try {
                        currentMessage.deleteFile();
                     } catch (Throwable error) {
                        ActiveMQServerLogger.LOGGER.errorDeletingLargeMessageFile(error);
                     } finally {
                        currentMessage = null;
                     }
                  }
               });
            }
         } catch (Exception ex) {
            logger.trace("AMQP Large Message reader close ignored error: ", ex);
         }

         inflater.reset();
         deliveryAnnotations = null;
         state = State.CLOSED;
      }
   }

   @Override
   public MessageReader open() {
      if (state != State.CLOSED) {
         throw new IllegalStateException("Message reader must be properly closed before open call");
      }

      state = State.INITIALIZING;

      return this;
   }

   @Override
   public Message readBytes(Delivery delivery) throws Exception {
      if (state == State.CLOSED) {
         throw new IllegalStateException("AMQP Compressed Large Message Reader is closed and read cannot proceed");
      }

      try {
         serverReceiver.connection.requireInHandler();
         serverReceiver.getConnection().disableAutoRead();

         final Receiver receiver = ((Receiver) delivery.getLink());
         final ReadableBuffer dataBuffer = receiver.recv();
         final AMQPSessionCallback sessionSPI = serverReceiver.getSessionContext().getSessionSPI();

         if (currentMessage == null) {
            final long id = sessionSPI.getStorageManager().generateID();
            final AMQPLargeMessage localCurrentMessage = new AMQPLargeMessage(id, delivery.getMessageFormat(), null, sessionSPI.getCoreMessageObjectPools(), sessionSPI.getStorageManager());

            localCurrentMessage.parseHeader(dataBuffer);

            sessionSPI.getStorageManager().onLargeMessageCreate(id, localCurrentMessage);
            currentMessage = localCurrentMessage;
         }

         sessionSPI.execute(() -> processRead(delivery, dataBuffer, delivery.isPartial()));

         return null; // Event fired once message is fully processed.
      } catch (Exception e) {
         serverReceiver.getConnection().enableAutoRead();
         throw e;
      }
   }

   private void processRead(Delivery delivery, ReadableBuffer dataBuffer, boolean isPartial) {
      final AMQPLargeMessage localCurrentMessage = currentMessage;

      // This method runs on the session thread and if the close is called and the scheduled file
      // delete occurs on the session thread first then current message will be null and we return.
      // But if the closed delete hasn't run first we can safely continue processing this message
      // in hopes we already read all the bytes before the connection was dropped.
      if (localCurrentMessage == null) {
         return;
      }

      // Store what we read into a composite as we may need to hold onto some or all of
      // the received data until a complete type is available.
      if (dataBuffer.hasRemaining()) {
         pendingRecvBuffer.append(dataBuffer);
      }

      try {
         while (pendingRecvBuffer.hasRemaining()) {
            pendingRecvBuffer.mark();

            try {
               if (state == State.BODY_INFLATING) {
                  inflateCurrentBodySection(delivery, pendingRecvBuffer);
               } else {
                  scanForNextMessageSection(pendingRecvBuffer);
               }

               // Advance mark so read bytes can be discarded and we can start from this
               // location next time.
               pendingRecvBuffer.mark();
            } catch (ActiveMQException ex) {
               throw ex;
            } catch (Exception e) {
               // We expect exceptions from proton when only partial section are received within
               // a frame so we will continue trying to decode until either we read a complete
               // section or we consume everything to the point of completing the delivery.
               if (delivery.isPartial()) {
                  pendingRecvBuffer.reset();
                  break; // Not enough data to decode yet.
               } else {
                  throw new ActiveMQAMQPInternalErrorException(
                     "Decoding error encounted in compressed AMQP large message.", e);
               }
            } finally {
               pendingRecvBuffer.reclaimRead();
            }
         }

         if (!delivery.isPartial()) {
            state = State.DONE;

            // We don't want a close to delete the file now, so we release these resources.
            localCurrentMessage.releaseResources(serverReceiver.getConnection().isLargeMessageSync(), true);
            currentMessage = null;
            serverReceiver.connection.runNow(() -> serverReceiver.onMessageComplete(delivery, localCurrentMessage, localCurrentMessage.getDeliveryAnnotations()));
         }
      } catch (Throwable e) {
         serverReceiver.onExceptionWhileReading(e);
      } finally {
         serverReceiver.connection.runNow(serverReceiver.getConnection()::enableAutoRead);
      }
   }

   // The caller has already stripped away the Data section and Binary encoding bits
   protected void inflateCurrentBodySection(Delivery delivery, CompositeReadableBuffer pendingRecvBuffer) throws Exception {
      while (!inflater.finished()) {
         if (inflater.needsInput()) {
            final int availableSize = Math.min(dataSectionRemaining, pendingRecvBuffer.remaining());
            final int readSize = Math.min(availableSize, inflateFrom.length);

            if (readSize == 0) {
               return; // No data currently available for input
            }

            pendingRecvBuffer.get(inflateFrom, 0, readSize);
            dataSectionRemaining -= readSize;

            if (dataSectionRemaining == 0) {
               state = State.BODY_SECTION_PENDING;
            }

            inflater.setInput(inflateFrom);
         }

         inflater.inflate(inflateTo);

         try {
            final ActiveMQBuffer bodyBuffer = ActiveMQBuffers.wrappedBuffer(inflateTo.flip());

            bodyBuffer.writerIndex(inflateTo.remaining());

            currentMessage.addBytes(bodyBuffer);
         } catch (ActiveMQException ex) {
            throw ex;
         } catch (Exception ex) {
            throw new ActiveMQAMQPInternalErrorException("Error while adding body bytes to AMQP Large message", ex);
         } finally {
            inflateTo.clear();
         }
      }
   }

   private void scanForNextMessageSection(ReadableBuffer buffer) throws ActiveMQException {
      final DecoderImpl decoder = TLSEncode.getDecoder();

      decoder.setBuffer(buffer);

      try {
         final TypeConstructor<?> constructor = decoder.readConstructor();

         if (Header.class.equals(constructor.getTypeClass())) {
            constructor.skipValue(); // Ignore the initial Header used for creating the Large message correctly.
         } else if (DeliveryAnnotations.class.equals(constructor.getTypeClass())) {
            deliveryAnnotations = (DeliveryAnnotations) constructor.readValue();
         } else if (MessageAnnotations.class.equals(constructor.getTypeClass())) {
            constructor.skipValue(); // Ignore for forward compatibility
         } else if (Properties.class.equals(constructor.getTypeClass())) {
            constructor.skipValue(); // Ignore for forward compatibility
         } else if (ApplicationProperties.class.equals(constructor.getTypeClass())) {
            constructor.skipValue(); // Ignore for forward compatibility
         } else if (Data.class.equals(constructor.getTypeClass())) {
            final int dataSectionSize = readNextDataSectionSize(pendingRecvBuffer);

            if (state.ordinal() < State.BODY_INFLATING.ordinal()) {
               dataSectionRemaining = dataSectionSize;
               inflater.reset();
               state = State.BODY_INFLATING;
            } else {
               throw new IllegalStateException("Data section found when not expecting any more input.");
            }
         } else if (AmqpValue.class.equals(constructor.getTypeClass())) {
            throw new IllegalArgumentException("Received an AmqpValue payload in compressed influght AMQP message");
         } else if (AmqpSequence.class.equals(constructor.getTypeClass())) {
            throw new IllegalArgumentException("Received an AmqpSequence payload in compressed inflight AMQP message");
         } else if (Footer.class.equals(constructor.getTypeClass())) {
            if (currentMessage == null) {
               throw new IllegalArgumentException("Received an Footer but no actual message paylod in compressed AMQP message");
            }

            constructor.skipValue(); // Ignore for forward compatibility
         }
      } finally {
         decoder.setBuffer(null);
      }
   }

   // This reads the size for the encoded Binary that should follow a detected Data section and
   // leaves the buffer at the head of the payload of the Binary, readers from here should just
   // use the returned size to determine when all the binary payload is consumed.
   private static int readNextDataSectionSize(ReadableBuffer buffer) throws ActiveMQException {
      final byte encodingCode = buffer.get();

      return switch (encodingCode) {
         case EncodingCodes.VBIN8 -> buffer.get() & 0xFF;
         case EncodingCodes.VBIN32 -> buffer.getInt();
         case EncodingCodes.NULL -> 0;
         default -> throw new ActiveMQException("Expected Binary type but found encoding: " + encodingCode);
      };
   }
}
