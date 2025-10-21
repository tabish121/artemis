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

import static org.apache.activemq.artemis.protocol.amqp.proton.AMQPArtemisMessageFormats.AMQP_COMPRESSED_MESSAGE_FORMAT;

import java.lang.invoke.MethodHandles;
import java.util.zip.Deflater;

import org.apache.activemq.artemis.core.server.MessageReference;
import org.apache.activemq.artemis.protocol.amqp.broker.AMQPMessage;
import org.apache.activemq.artemis.protocol.amqp.broker.AMQPSessionCallback;
import org.apache.activemq.artemis.protocol.amqp.broker.ActiveMQProtonRemotingConnection;
import org.apache.activemq.artemis.protocol.amqp.converter.CoreAmqpConverter;
import org.apache.activemq.artemis.protocol.amqp.util.NettyReadable;
import org.apache.activemq.artemis.protocol.amqp.util.NettyWritable;
import org.apache.activemq.artemis.protocol.amqp.util.TLSEncode;
import org.apache.qpid.proton.amqp.messaging.DeliveryAnnotations;
import org.apache.qpid.proton.codec.EncoderImpl;
import org.apache.qpid.proton.codec.EncodingCodes;
import org.apache.qpid.proton.codec.ReadableBuffer;
import org.apache.qpid.proton.codec.WritableBuffer;
import org.apache.qpid.proton.engine.Delivery;
import org.apache.qpid.proton.engine.EndpointState;
import org.apache.qpid.proton.engine.Sender;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

/**
 * An writer of AMQP (non-large) messages or messages which will convert any non-AMQP message to AMQP before writing the
 * encoded bytes into the AMQP sender.
 */
public class AMQPMessageDeflatingWriter implements MessageWriter {

   private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

   private static final byte DATA_DESCRIPTOR = 0x75;
   private static final int DEFLATE_MIN_WRITE_LIMIT = 2048;

   private final ProtonServerSenderContext serverSender;
   private final AMQPSessionCallback sessionSPI;
   private final Sender protonSender;
   private final Deflater deflater = new Deflater();

   public AMQPMessageDeflatingWriter(ProtonServerSenderContext serverSender) {
      this.serverSender = serverSender;
      this.sessionSPI = serverSender.getSessionContext().getSessionSPI();
      this.protonSender = serverSender.getSender();
   }

   @Override
   public void close() {
      deflater.reset();
   }

   @Override
   public void writeBytes(MessageReference messageReference) {
      if (protonSender.getLocalState() == EndpointState.CLOSED) {
         logger.debug("Not delivering message {} as the sender is closed and credits were available, if you see too many of these it means clients are issuing credits and closing the connection with pending credits a lot of times", messageReference);
         return;
      }

      try {
         final AMQPMessage amqpMessage = CoreAmqpConverter.checkAMQP(messageReference.getMessage(), null);

         if (sessionSPI.invokeOutgoing(amqpMessage, (ActiveMQProtonRemotingConnection) sessionSPI.getTransportConnection().getProtocolConnection()) != null) {
            return;
         }

         final ByteBuf encodedMessage = Unpooled.buffer();
         final Delivery delivery = serverSender.createDelivery(messageReference, AMQP_COMPRESSED_MESSAGE_FORMAT);
         final ReadableBuffer messageBuffer = amqpMessage.getSendBuffer(messageReference.getDeliveryCount(), messageReference, null);

         // Write uncompressed delivery annotations to preserve the option to add compression related annotations into
         // these annotations without needing to possibly encode two sets.
         final DeliveryAnnotations annotations = messageReference.getProtocolData(DeliveryAnnotations.class);
         if (annotations != null && annotations.getValue() != null && !annotations.getValue().isEmpty()) {
            final EncoderImpl encoder = TLSEncode.getEncoder();

            try {
               encoder.setByteBuffer(new NettyWritable(encodedMessage));
               encoder.writeObject(annotations);
            } finally {
               encoder.setByteBuffer((WritableBuffer) null);
            }
         }

         // This encoding would work up to an AMQP message that encodes to but does not exceed
         // 2 GB in which case we'd need to send multiple data sections but this would be unlikely
         // to succeed and Large message handling should have been in place for such messages.

         final int sizeIndex = encodedMessage.writerIndex() + Integer.BYTES;

         encodedMessage.writeByte(EncodingCodes.DESCRIBED_TYPE_INDICATOR);
         encodedMessage.writeByte(EncodingCodes.SMALLULONG);
         encodedMessage.writeByte(DATA_DESCRIPTOR);
         encodedMessage.writeByte(EncodingCodes.VBIN32);
         encodedMessage.writeInt(0);

         final int compressedSize = deflateMessageBytes(messageBuffer, encodedMessage);

         // Update the buffer with the size of the data section after compressing the message.
         encodedMessage.setInt(sizeIndex, compressedSize);

         // We wrote into an un-pooled buffer so we just hand off the bytes directly.
         protonSender.sendNoCopy(new NettyReadable(encodedMessage));

         serverSender.reportDeliveryComplete(this, messageReference, delivery, false);
      } catch (Exception deliveryError) {
         serverSender.reportDeliveryError(this, messageReference, deliveryError);
      }
   }

   protected int deflateMessageBytes(ReadableBuffer messageBytes, ByteBuf compressedBytes) {
      if (messageBytes.hasArray()) {
         deflater.setInput(messageBytes.array(), messageBytes.arrayOffset(), messageBytes.remaining());
      } else {
         deflater.setInput(messageBytes.byteBuffer());
      }

      deflater.finish();

      int deflateResult = 0;
      int compressedSize = 0;

      while (!deflater.finished()) {
         // Ensure the buffer has at least some minimum number of writable bytes to deflate to, the actual
         // amount could be more but we want at least some space for the deflate to operate with.
         compressedBytes.ensureWritable(DEFLATE_MIN_WRITE_LIMIT);

         compressedSize += deflateResult =
            deflater.deflate(compressedBytes.internalNioBuffer(compressedBytes.writerIndex(), compressedBytes.writableBytes()));

         compressedBytes.writerIndex(compressedBytes.writerIndex() + deflateResult);
      }

      return compressedSize;
   }
}
