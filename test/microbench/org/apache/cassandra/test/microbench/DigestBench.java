/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.test.microbench;

import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;

import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.MD5Digest;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

import sun.security.provider.MD5;

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 2, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 3, time = 2, timeUnit = TimeUnit.SECONDS)
@Threads(1)
@Fork(value = 1,jvmArgsAppend = "-Xmx50M")

@State(Scope.Benchmark)
public class DigestBench
{

    @Param({"512", "1024"})
    public int buffSize;

    byte[] expected;
    byte[] expectedIntDigest;
    ByteBuffer[] dummyValues;

    class BufferedDigest extends MessageDigest
    {
        ByteBuffer buffer;
        private final MessageDigest underlying;

        public BufferedDigest(MessageDigest underlying, int bufferSize)
        {
            super(underlying.getAlgorithm());
            this.underlying = underlying;
            this.buffer = ByteBuffer.allocate(bufferSize);
        }

        @Override
        protected void engineUpdate(byte input)
        {
            if (!buffer.hasRemaining())
            {
                buffer.flip();
                underlying.update(buffer);
                buffer.clear();
            }
            buffer.put(input);
        }

        @Override
        protected void engineUpdate(byte[] input, int offset, int len)
        {
            if (len > buffer.remaining())
            {
                if (buffer.position() > 0)
                {
                    buffer.flip();
                    underlying.update(buffer);
                    buffer.clear();
                }
                underlying.update(input, offset, len);
            }
            else
            {
                buffer.put(input, offset, len);
            }
        }

        @Override
        protected byte[] engineDigest()
        {
            if (buffer.position() != 0)
            {
                buffer.flip();
                underlying.update(buffer);
                buffer.clear();
            }
            return underlying.digest();
        }

        @Override
        protected void engineReset()
        {
            underlying.reset();
            buffer.clear();
        }
    }

    class SimpleBufferedDigest extends MessageDigest
    {
        public byte[] buffer;
        public int pos;
        private MessageDigest underlying;

        public SimpleBufferedDigest(MessageDigest underlying, int bufferSize)
        {
            super(underlying.getAlgorithm());
            this.underlying = underlying;
            this.buffer = new byte[bufferSize];
            this.pos = 0;
        }

        @Override
        protected void engineUpdate(byte input)
        {
            if (pos >= buffer.length)
            {
                underlying.update(buffer);
                pos = 0;
            }
            buffer[pos++] = input;
        }

        @Override
        protected void engineUpdate(byte[] input, int offset, int len)
        {
            if (len > (buffer.length - pos))
            {
                if (pos > 0)
                {
                    underlying.update(buffer, 0, pos);
                    pos = 0;
                }
                underlying.update(input, offset, len);
            }
            else
            {
                System.arraycopy(input, offset, buffer, pos, len);
            }
        }

        @Override
        protected byte[] engineDigest()
        {
            if (pos > 0)
            {
                underlying.update(buffer, 0, pos);
                pos = 0;
            }
            return underlying.digest();
        }

        @Override
        protected void engineReset()
        {
            underlying.reset();
            pos = 0;
        }
    }
    @Setup
    public void setup() throws NoSuchAlgorithmException
    {
        MessageDigest digest = MessageDigest.getInstance("MD5");
        MessageDigest digest2 = MessageDigest.getInstance("MD5");
        dummyValues = new ByteBuffer[10000];
        for(long i = 0; i < 10000; i ++)
        {
            dummyValues[(int)i] = ByteBuffer.allocate(64);
            for(int j = 0; j < 8; j ++)
            {
                dummyValues[(int)i].putLong(i);
            }

            digest.update(dummyValues[(int)i].duplicate());
            FBUtilities.updateWithLong(digest, 12L);
            FBUtilities.updateWithInt(digest, 1245);
            FBUtilities.updateWithBoolean(digest, false);
            FBUtilities.updateWithInt(digest2, (int)i);
        }
        expected = digest.digest();
        expectedIntDigest = digest2.digest();
    }

    @Benchmark
    public void oldDigest() throws NoSuchAlgorithmException
    {
        MessageDigest digest = MessageDigest.getInstance("MD5");
        for(long i = 0; i < 10000; i ++)
        {
            digest.update(dummyValues[(int)i].duplicate());
            FBUtilities.updateWithLong(digest, 12L);
            FBUtilities.updateWithInt(digest, 1245);
            FBUtilities.updateWithBoolean(digest, false);
        }
        assert Arrays.equals(expected, digest.digest());
    }

    @Benchmark
    public void oldDigestBuffered() throws NoSuchAlgorithmException
    {
        MessageDigest digest = new BufferedDigest(MessageDigest.getInstance("MD5"), buffSize);
        for(long i = 0; i < 10000; i ++)
        {
            digest.update(dummyValues[(int)i].duplicate());
            FBUtilities.updateWithLong(digest, 12L);
            FBUtilities.updateWithInt(digest, 1245);
            FBUtilities.updateWithBoolean(digest, false);
        }
        assert Arrays.equals(expected, digest.digest());
    }

    @Benchmark
    public void oldDigestSimpleBuffered() throws NoSuchAlgorithmException
    {
        MessageDigest digest = new SimpleBufferedDigest(MessageDigest.getInstance("MD5"), buffSize);
        for(long i = 0; i < 10000; i ++)
        {
            digest.update(dummyValues[(int)i].duplicate());
            FBUtilities.updateWithLong(digest, 12L);
            FBUtilities.updateWithInt(digest, 1245);
            FBUtilities.updateWithBoolean(digest, false);
        }
        assert Arrays.equals(expected, digest.digest());
    }

    @Benchmark
    public void newDigest() throws NoSuchAlgorithmException
    {
        MessageDigest digest = new SimpleBufferedDigest(MessageDigest.getInstance("MD5"), buffSize);

        for(long i = 0; i < 10000; i++)
        {
            int dataLength = 0 ;//dummyValues[(int)i].array().length;
            byte[] buf = new byte[dataLength + 13];

            digest.update(dummyValues[(int)i].duplicate());

            FBUtilities.convertLongToBytes(12L, buf, dataLength );
            FBUtilities.convertIntToBytes(1245, buf, dataLength + 8);
            buf[dataLength + 12] = (byte) 0;
            digest.update(buf);
        }
        assert Arrays.equals(expected, digest.digest());
    }

    @Benchmark
    public void updateWithInt() throws NoSuchAlgorithmException
    {
        MessageDigest digest = MessageDigest.getInstance("MD5");
        for(int i = 0; i < 10000; i++)
        {
            FBUtilities.updateWithInt(digest, i);
        }
        Assert.assertArrayEquals(expectedIntDigest, digest.digest());
    }

    @Benchmark
    public void bufferedUpdateWithInt() throws NoSuchAlgorithmException
    {
        MessageDigest digest = new BufferedDigest(MessageDigest.getInstance("MD5"), buffSize);
        for(int i = 0; i < 10000; i++)
        {
            FBUtilities.updateWithInt(digest, i);
        }
        Assert.assertArrayEquals(expectedIntDigest, digest.digest());
    }

    @Benchmark
    public void convertIntToBytes() throws NoSuchAlgorithmException
    {
        MessageDigest digest = MessageDigest.getInstance("MD5");
        ByteBuffer val = ByteBuffer.allocate(4);

        for(int i = 0; i < 10000; i++)
        {
            val.putInt(0, i);
            digest.update(val.duplicate().slice());
        }
    }

    @Benchmark
    public void byteBufferDigest() throws NoSuchAlgorithmException
    {
        MessageDigest digest = MessageDigest.getInstance("MD5");

        for(long i = 0; i < 10000; i++)
        {
            digest.update(dummyValues[(int)i].duplicate());
            ByteBuffer buf = ByteBuffer.allocate(13);
            buf.putLong(0, 12L);
            buf.putInt(8, 1245);
            buf.put(12, (byte) 0);
            digest.update(buf);
        }
        assert Arrays.equals(expected, digest.digest());
    }
}
