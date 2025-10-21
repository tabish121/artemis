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

import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.api.core.ActiveMQBuffers;
import org.apache.qpid.proton.amqp.Binary;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

/**
 * Reader of tunneled Core message that have been written as the body of an AMQP delivery with a custom message format
 * that indicates this payload. The reader will extract bytes from the delivery and decode from them a standard Core
 * message which is then routed into the broker as if received from a Core connection.
 */
public class AMQPTunneledCoreMessageInflatingReader extends AMQPTunneledCoreMessageReader {

   private static final int INFLATE_MIN_WRITE_LIMIT = 2048;

   private final Inflater inflater = new Inflater();

   public AMQPTunneledCoreMessageInflatingReader(ProtonAbstractReceiver serverReceiver) {
      super(serverReceiver);
   }

   @Override
   public void close() {
      inflater.reset();
      super.close();
   }

   @Override
   protected ActiveMQBuffer extractBufferFromBinary(Binary payloadBinary) throws Exception {
      final ByteBuffer input = payloadBinary.asByteBuffer();
      final ByteBuf output = Unpooled.buffer(input.remaining());

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

      final ActiveMQBuffer buffer = ActiveMQBuffers.wrappedBuffer(output);

      // Ensure the wrapped buffer readable bytes reflects the decompressed data available.
      buffer.writerIndex(output.readableBytes());

      return buffer;
   }
}
