/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.client;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.util.CharsetUtil;
import org.apache.ignite.lang.IgniteException;
import org.msgpack.core.MessageFormat;
import org.msgpack.core.MessagePack;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

/**
 * Decodes full client messages:
 * 1. MAGIC for first message.
 * 2. Payload length (varint).
 * 3. Payload (bytes).
 */
public class ClientMessageDecoder extends ByteToMessageDecoder {
    /** Magic bytes before handshake. */
    public static final byte[] MAGIC_BYTES = new byte[]{0x49, 0x47, 0x4E, 0x49}; // IGNI

    /** */
    private byte[] data = new byte[4]; // TODO: Pooled buffers.

    /** */
    private int cnt = -4;

    /** */
    private int msgSize = -1;

    /** */
    private boolean magicDecoded;

    /** */
    private boolean magicFailed;

    /** */
    private MessageFormat sizeFormat = null;

    /** */
    private byte firstByte;

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf byteBuf, List<Object> list)
            throws IOException {
        if (!readMagic(byteBuf))
            return;

        while (read(byteBuf))
            list.add(ByteBuffer.wrap(data));
    }

    private boolean readMagic(ByteBuf byteBuf) {
        if (magicFailed)
            return false;

        if (magicDecoded)
            return true;

        if (byteBuf.readableBytes() < MAGIC_BYTES.length)
            return false;

        byteBuf.readBytes(data, 0, MAGIC_BYTES.length);

        magicDecoded = true;
        cnt = -1;
        msgSize = 0;

        if (Arrays.equals(data, MAGIC_BYTES))
            return true;

        magicFailed = true;

        throw new IgniteException("Invalid magic header in thin client connection. " +
                "Expected 'IGNI', but was '" + new String(data, CharsetUtil.US_ASCII) + "'.");
    }

    /**
     * Reads the buffer.
     *
     * @param buf Buffer.
     * @return True when a complete message has been received; false otherwise.
     */
    private boolean read(ByteBuf buf) throws IOException {
        if (buf.readableBytes() == 0)
            return false;

        if (cnt < 0) {
            // Read varint message size.
            if (sizeFormat == null) {
                firstByte = buf.readByte();
                sizeFormat = MessageFormat.valueOf(firstByte);
            }

            switch (sizeFormat) {
                case POSFIXINT:
                    msgSize = firstByte;
                    break;

                case INT8:
                case UINT8:
                    if (buf.readableBytes() < 1)
                        return false;

                    msgSize = buf.readUnsignedByte();
                    break;

                case INT16:
                case UINT16:
                    if (buf.readableBytes() < 2)
                        return false;

                    msgSize = buf.readUnsignedShort();
                    break;

                case INT32:
                case UINT32:
                    if (buf.readableBytes() < 4)
                        return false;

                    var size = buf.readUnsignedInt();

                    if (size > Integer.MAX_VALUE)
                        throw new IgniteException("Message too long: " + size);

                    msgSize = (int) size;
                    break;

                default:
                    throw new IgniteException("Unexpected message length format: " + sizeFormat);
            }

            assert msgSize >= 0;
            data = new byte[msgSize];
            cnt = 0;
        }

        assert data != null;
        assert msgSize >= 0;

        int remaining = buf.readableBytes();

        if (remaining > 0) {
            int missing = msgSize - cnt;

            if (missing > 0) {
                int len = Math.min(missing, remaining);

                buf.readBytes(data, cnt, len);

                cnt += len;
            }
        }

        if (cnt == msgSize) {
            cnt = -1;
            msgSize = -1;
            sizeFormat = null;

            return true;
        }

        return false;
    }
}
