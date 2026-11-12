/*
 * Copyright 2026 The Netty Project
 *
 * The Netty Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package io.netty.handler.codec.spdy;

import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.TooLongFrameException;
import io.netty.handler.codec.http.FullHttpMessage;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.util.CharsetUtil;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class SpdyHttpDecoderTest {

    private static final int MAX_CONTENT_LENGTH = 1024;

    private static final class TestSpdyHttpDecoder extends SpdyHttpDecoder {
        final List<FullHttpMessage> stored = new ArrayList<FullHttpMessage>();

        TestSpdyHttpDecoder() {
            super(SpdyVersion.SPDY_3_1, MAX_CONTENT_LENGTH);
        }

        @Override
        protected FullHttpMessage putMessage(int streamId, FullHttpMessage message) {
            stored.add(message);
            return super.putMessage(streamId, message);
        }
    }

    private static SpdySynStreamFrame newSynStream(int streamId, boolean last) {
        SpdySynStreamFrame frame = new DefaultSpdySynStreamFrame(streamId, 0, (byte) 0);
        frame.headers().set(SpdyHeaders.HttpNames.METHOD, "POST");
        frame.headers().set(SpdyHeaders.HttpNames.PATH, "/upload");
        frame.headers().set(SpdyHeaders.HttpNames.VERSION, "HTTP/1.1");
        frame.headers().set(SpdyHeaders.HttpNames.HOST, "example.com");
        frame.headers().set(SpdyHeaders.HttpNames.SCHEME, "https");
        frame.setLast(last);
        return frame;
    }

    private static SpdySynReplyFrame newSynReply(int streamId, boolean last) {
        SpdySynReplyFrame frame = new DefaultSpdySynReplyFrame(streamId);
        frame.headers().set(SpdyHeaders.HttpNames.STATUS, "200 OK");
        frame.headers().set(SpdyHeaders.HttpNames.VERSION, "HTTP/1.1");
        frame.setLast(last);
        return frame;
    }

    @Test
    public void testRstStreamReleasesPendingMessage() {
        TestSpdyHttpDecoder decoder = new TestSpdyHttpDecoder();
        EmbeddedChannel ch = new EmbeddedChannel(decoder);

        assertFalse(ch.writeInbound(newSynStream(1, false)));
        assertEquals(1, decoder.stored.size());
        FullHttpMessage pending = decoder.stored.get(0);
        assertEquals(1, pending.refCnt());

        assertFalse(ch.writeInbound(new DefaultSpdyRstStreamFrame(1, SpdyStreamStatus.CANCEL)));
        assertEquals(0, pending.refCnt());

        assertFalse(ch.finish());
    }

    @Test
    public void testRepeatedRstStreamCyclesDoNotLeak() {
        TestSpdyHttpDecoder decoder = new TestSpdyHttpDecoder();
        EmbeddedChannel ch = new EmbeddedChannel(decoder);

        for (int i = 0; i < 16; i++) {
            int streamId = 2 * i + 1;
            assertFalse(ch.writeInbound(newSynStream(streamId, false)));
            assertFalse(ch.writeInbound(new DefaultSpdyRstStreamFrame(streamId, SpdyStreamStatus.CANCEL)));
        }
        assertEquals(16, decoder.stored.size());
        for (FullHttpMessage message : decoder.stored) {
            assertEquals(0, message.refCnt());
        }

        assertFalse(ch.finish());
    }

    @Test
    public void testDuplicateSynStreamReleasesReplacedMessage() {
        TestSpdyHttpDecoder decoder = new TestSpdyHttpDecoder();
        EmbeddedChannel ch = new EmbeddedChannel(decoder);

        assertFalse(ch.writeInbound(newSynStream(1, false)));
        assertFalse(ch.writeInbound(newSynStream(1, false)));
        assertEquals(2, decoder.stored.size());
        FullHttpMessage replaced = decoder.stored.get(0);
        FullHttpMessage current = decoder.stored.get(1);
        assertEquals(0, replaced.refCnt());
        assertEquals(1, current.refCnt());

        assertFalse(ch.writeInbound(new DefaultSpdyRstStreamFrame(1, SpdyStreamStatus.CANCEL)));
        assertEquals(0, current.refCnt());

        assertFalse(ch.finish());
    }

    @Test
    public void testDuplicateSynReplyReleasesReplacedMessage() {
        TestSpdyHttpDecoder decoder = new TestSpdyHttpDecoder();
        EmbeddedChannel ch = new EmbeddedChannel(decoder);

        assertFalse(ch.writeInbound(newSynReply(1, false)));
        assertFalse(ch.writeInbound(newSynReply(1, false)));
        assertEquals(2, decoder.stored.size());
        FullHttpMessage replaced = decoder.stored.get(0);
        FullHttpMessage current = decoder.stored.get(1);
        assertEquals(0, replaced.refCnt());
        assertEquals(1, current.refCnt());

        assertFalse(ch.writeInbound(new DefaultSpdyRstStreamFrame(1, SpdyStreamStatus.CANCEL)));
        assertEquals(0, current.refCnt());

        assertFalse(ch.finish());
    }

    @Test
    public void testTooLongContentReleasesPendingMessage() {
        TestSpdyHttpDecoder decoder = new TestSpdyHttpDecoder();
        final EmbeddedChannel ch = new EmbeddedChannel(decoder);

        assertFalse(ch.writeInbound(newSynStream(1, false)));
        assertEquals(1, decoder.stored.size());
        FullHttpMessage pending = decoder.stored.get(0);

        final SpdyDataFrame tooLong =
                new DefaultSpdyDataFrame(1, Unpooled.wrappedBuffer(new byte[MAX_CONTENT_LENGTH + 1]));
        assertThrows(TooLongFrameException.class, new Executable() {
            @Override
            public void execute() {
                ch.writeInbound(tooLong);
            }
        });
        assertEquals(0, tooLong.refCnt());
        assertEquals(0, pending.refCnt());

        assertFalse(ch.finish());
    }

    @Test
    public void testHandlerRemovedReleasesPendingMessages() {
        TestSpdyHttpDecoder decoder = new TestSpdyHttpDecoder();
        EmbeddedChannel ch = new EmbeddedChannel(decoder);

        assertFalse(ch.writeInbound(newSynStream(1, false)));
        assertFalse(ch.writeInbound(newSynReply(3, false)));
        assertEquals(2, decoder.stored.size());

        ch.pipeline().remove(decoder);
        for (FullHttpMessage message : decoder.stored) {
            assertEquals(0, message.refCnt());
        }

        assertFalse(ch.finish());
    }

    @Test
    public void testChannelInactiveReleasesPendingMessages() {
        TestSpdyHttpDecoder decoder = new TestSpdyHttpDecoder();
        EmbeddedChannel ch = new EmbeddedChannel(decoder);

        assertFalse(ch.writeInbound(newSynStream(1, false)));
        assertEquals(1, decoder.stored.size());
        FullHttpMessage pending = decoder.stored.get(0);

        // Closing fires channelInactive and then handlerRemoved; the message must be released exactly once.
        assertFalse(ch.finish());
        assertEquals(0, pending.refCnt());
    }

    @Test
    public void testCompleteRequestIsForwardedNotReleased() {
        TestSpdyHttpDecoder decoder = new TestSpdyHttpDecoder();
        EmbeddedChannel ch = new EmbeddedChannel(decoder);

        assertFalse(ch.writeInbound(newSynStream(1, false)));
        SpdyDataFrame data = new DefaultSpdyDataFrame(1, Unpooled.copiedBuffer("hello", CharsetUtil.US_ASCII));
        data.setLast(true);
        assertTrue(ch.writeInbound(data));

        FullHttpRequest request = ch.readInbound();
        assertEquals(1, request.refCnt());
        assertEquals("hello", request.content().toString(CharsetUtil.US_ASCII));
        assertTrue(request.release());
        assertNull(ch.readInbound());

        assertFalse(ch.finish());
    }
}
