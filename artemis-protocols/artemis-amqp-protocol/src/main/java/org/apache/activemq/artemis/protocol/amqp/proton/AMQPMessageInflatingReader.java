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

import java.nio.ByteBuffer;
import java.util.zip.Inflater;

import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.protocol.amqp.broker.AMQPMessage;
import org.apache.activemq.artemis.protocol.amqp.util.NettyReadable;
import org.apache.activemq.artemis.protocol.amqp.util.TLSEncode;
import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.messaging.AmqpSequence;
import org.apache.qpid.proton.amqp.messaging.AmqpValue;
import org.apache.qpid.proton.amqp.messaging.ApplicationProperties;
import org.apache.qpid.proton.amqp.messaging.Data;
import org.apache.qpid.proton.amqp.messaging.DeliveryAnnotations;
import org.apache.qpid.proton.amqp.messaging.Footer;
import org.apache.qpid.proton.amqp.messaging.Header;
import org.apache.qpid.proton.amqp.messaging.MessageAnnotations;
import org.apache.qpid.proton.amqp.messaging.Properties;
import org.apache.qpid.proton.codec.DecoderImpl;
import org.apache.qpid.proton.codec.ReadableBuffer;
import org.apache.qpid.proton.codec.TypeConstructor;
import org.apache.qpid.proton.engine.Delivery;
import org.apache.qpid.proton.engine.Receiver;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

/**
 * Reader of AMQP (non-large) messages which reads all bytes and decodes once a non-partial
 * delivery is read.
 */
public class AMQPMessageInflatingReader implements MessageReader {

   private static final int INFLATE_MIN_WRITE_LIMIT = 2048;

   private final ProtonAbstractReceiver serverReceiver;
   private final Inflater inflater = new Inflater();

   private DeliveryAnnotations deliveryAnnotations;
   private boolean closed = true;

   public AMQPMessageInflatingReader(ProtonAbstractReceiver serverReceiver) {
      this.serverReceiver = serverReceiver;
   }

   @Override
   public DeliveryAnnotations getDeliveryAnnotations() {
      return deliveryAnnotations;
   }

   @Override
   public void close() {
      inflater.reset();
      closed = true;
      deliveryAnnotations = null;
   }

   @Override
   public MessageReader open() {
      if (!closed) {
         throw new IllegalStateException("Message reader must be properly closed before open call");
      }

      return this;
   }

   @Override
   public Message readBytes(Delivery delivery) throws Exception {
      if (delivery.isPartial()) {
         return null; // Only receive payload when complete
      }

      // The scan will pickup the delivery annotations as part of the process of searching for the
      // message body data section containing the compressed bytes.
      final Binary payloadBinary = scanForMessagePayload(delivery);
      final ReadableBuffer payload = inflateBufferFromBinary(payloadBinary);
      final AMQPMessage message = serverReceiver.getSessionContext().getSessionSPI().createStandardMessage(delivery, payload);

      return message;
   }

   protected Binary scanForMessagePayload(Delivery delivery) {
      final Receiver receiver = ((Receiver) delivery.getLink());
      final ReadableBuffer recievedBuffer = receiver.recv();

      if (recievedBuffer.remaining() == 0) {
         throw new IllegalArgumentException("Received empty delivery when expecting a compressed AMQP message encoding");
      }

      final DecoderImpl decoder = TLSEncode.getDecoder();

      decoder.setBuffer(recievedBuffer);

      Data payloadData = null;

      try {
         while (recievedBuffer.hasRemaining()) {
            final TypeConstructor<?> constructor = decoder.readConstructor();

            if (Header.class.equals(constructor.getTypeClass())) {
               constructor.skipValue(); // Ignore for forward compatibility
            } else if (DeliveryAnnotations.class.equals(constructor.getTypeClass())) {
               deliveryAnnotations = (DeliveryAnnotations) constructor.readValue();
            } else if (MessageAnnotations.class.equals(constructor.getTypeClass())) {
               constructor.skipValue(); // Ignore for forward compatibility
            } else if (Properties.class.equals(constructor.getTypeClass())) {
               constructor.skipValue(); // Ignore for forward compatibility
            } else if (ApplicationProperties.class.equals(constructor.getTypeClass())) {
               constructor.skipValue(); // Ignore for forward compatibility
            } else if (Data.class.equals(constructor.getTypeClass())) {
               if (payloadData != null) {
                  throw new IllegalArgumentException("Received an unexpected additional Data section in compressed AMQP message");
               }

               payloadData = (Data) constructor.readValue();
            } else if (AmqpValue.class.equals(constructor.getTypeClass())) {
               throw new IllegalArgumentException("Received an AmqpValue payload in compressed AMQP message");
            } else if (AmqpSequence.class.equals(constructor.getTypeClass())) {
               throw new IllegalArgumentException("Received an AmqpSequence payload in compressed AMQP message");
            } else if (Footer.class.equals(constructor.getTypeClass())) {
               if (payloadData == null) {
                  throw new IllegalArgumentException("Received a Footer but no actual message payload in compressed AMQP message");
               }

               constructor.skipValue(); // Ignore for forward compatibility
            }
         }
      } finally {
         decoder.setBuffer(null);
      }

      if (payloadData == null) {
         throw new IllegalArgumentException("Did not receive a Data section payload in compressed AMQP message");
      }

      final Binary payloadBinary = payloadData.getValue();

      if (payloadBinary == null || payloadBinary.getLength() <= 0) {
         throw new IllegalArgumentException("Received an unexpected empty message payload in core tunneled AMQP message");
      }

      return payloadBinary;
   }

   protected ReadableBuffer inflateBufferFromBinary(Binary payloadBinary) throws Exception {
      final ByteBuffer input = payloadBinary.asByteBuffer();
      final ByteBuf output = Unpooled.buffer();

      if (input.hasArray()) {
         inflater.setInput(input.array(), input.arrayOffset(), input.remaining());
      } else {
         inflater.setInput(input);
      }

      int inflateResult = 0;

      while (!inflater.finished()) {
         // Provide some realized capacity that can be used to write the inflated bytes into
         output.ensureWritable(INFLATE_MIN_WRITE_LIMIT);

         inflateResult = inflater.inflate(output.internalNioBuffer(output.writerIndex(), output.writableBytes()));

         output.writerIndex(output.writerIndex() + inflateResult);
      }

      return new NettyReadable(output);
   }
}
