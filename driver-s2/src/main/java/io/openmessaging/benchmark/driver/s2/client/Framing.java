/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.openmessaging.benchmark.driver.s2.client;


import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPInputStream;

/**
 * Codec for the S2 "s2s" session protocol framing.
 *
 * <p>Each message is: a 3-byte big-endian length prefix covering (flag byte + body), a 1-byte flag
 * {@code [T][CC][RRRRR]} where T is the terminal bit and CC is the compression scheme, followed by
 * the body. Regular bodies are protobuf messages. Terminal bodies are a 2-byte big-endian status
 * code followed by JSON error information.
 */
public final class Framing {

    public static final byte FLAG_TERMINAL = (byte) 0b1000_0000;
    public static final byte COMPRESSION_NONE = 0b00;
    public static final byte COMPRESSION_ZSTD = 0b01;
    public static final byte COMPRESSION_GZIP = 0b10;

    private Framing() {}

    /** A decoded s2s frame. */
    public static final class Frame {
        public final boolean terminal;
        public final byte[] body;

        Frame(boolean terminal, byte[] body) {
            this.terminal = terminal;
            this.body = body;
        }

        // Only valid when the terminal flag is set.
        public int statusCode() {
            return ((body[0] & 0xff) << 8) | (body[1] & 0xff);
        }

        // Only valid when the terminal flag is set.
        public String errorJson() {
            return new String(body, 2, body.length - 2, StandardCharsets.UTF_8);
        }
    }

    // Encode an uncompressed non-terminal frame around the given protobuf body.
    public static ByteBuffer encode(byte[] body) {
        int len = body.length + 1;
        if (len > 0xff_ffff) {
            throw new IllegalArgumentException("frame body too large: " + body.length);
        }
        ByteBuffer buf = ByteBuffer.allocate(3 + len);
        buf.put((byte) ((len >>> 16) & 0xff));
        buf.put((byte) ((len >>> 8) & 0xff));
        buf.put((byte) (len & 0xff));
        buf.put((byte) 0);
        buf.put(body);
        buf.flip();
        return buf;
    }

    /** Incremental decoder: feed arbitrary chunks, emits complete frames. */
    public static final class Decoder {
        private final ByteArrayOutputStream pending = new ByteArrayOutputStream();

        public List<Frame> feed(ByteBuffer chunk) throws IOException {
            byte[] in = new byte[chunk.remaining()];
            chunk.get(in);
            pending.write(in);
            List<Frame> frames = new ArrayList<>();
            byte[] buf = pending.toByteArray();
            int off = 0;
            while (buf.length - off >= 3) {
                int len = ((buf[off] & 0xff) << 16) | ((buf[off + 1] & 0xff) << 8) | (buf[off + 2] & 0xff);
                if (buf.length - off - 3 < len) {
                    break;
                }
                byte flag = buf[off + 3];
                byte[] body = new byte[len - 1];
                System.arraycopy(buf, off + 4, body, 0, len - 1);
                boolean terminal = (flag & FLAG_TERMINAL) != 0;
                int compression = (flag >> 5) & 0b11;
                frames.add(new Frame(terminal, decompress(compression, body)));
                off += 3 + len;
            }
            pending.reset();
            pending.write(buf, off, buf.length - off);
            return frames;
        }

        private static byte[] decompress(int compression, byte[] body) throws IOException {
            switch (compression) {
                case COMPRESSION_NONE:
                    return body;
                case COMPRESSION_GZIP:
                    try (GZIPInputStream gz = new GZIPInputStream(new java.io.ByteArrayInputStream(body))) {
                        return readAll(gz);
                    }
                default:
                    throw new IOException("unsupported s2s compression scheme: " + compression);
            }
        }

        private static byte[] readAll(InputStream in) throws IOException {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            byte[] tmp = new byte[8192];
            int n;
            while ((n = in.read(tmp)) != -1) {
                out.write(tmp, 0, n);
            }
            return out.toByteArray();
        }
    }
}
