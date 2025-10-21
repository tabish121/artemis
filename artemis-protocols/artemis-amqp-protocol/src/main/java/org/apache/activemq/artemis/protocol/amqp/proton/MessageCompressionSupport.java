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

import org.apache.activemq.artemis.protocol.amqp.util.NettyReadable;
import org.apache.qpid.proton.codec.ReadableBuffer;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

public abstract class MessageCompressionSupport {

   private MessageCompressionSupport() {
      // Hide to prevent create
   }

   protected static ReadableBuffer inflateDeliveredBytees(Inflater inflater, byte[] scratchBuffer, ByteBuffer buffer) throws Exception {
      final ReadableBuffer input = new ReadableBuffer.ByteBufferReader(buffer);
      final ByteBuf output = Unpooled.buffer();

      if (input.hasArray()) {
         inflater.setInput(input.array(), input.arrayOffset(), input.remaining());
      } else {
         inflater.setInput(input.byteBuffer());
      }

      while (!inflater.finished() && !inflater.needsInput()) {
         int decompressedSize = inflater.inflate(scratchBuffer);
         output.writeBytes(scratchBuffer, 0, decompressedSize);
      }

      return new NettyReadable(output);
   }
}
