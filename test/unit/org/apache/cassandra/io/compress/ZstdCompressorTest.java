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

package org.apache.cassandra.io.compress;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import static java.util.UUID.randomUUID;
import static org.junit.Assert.*;

public class ZstdCompressorTest
{
    static ICompressor compressor;
    static ICompressor dictCompressor;


    @Before
    public void setup()
    {
         compressor = ZstdCompressor.create(Collections.emptyMap());
         Map<String, String> test = new HashMap<>();

         test.put("dictionary_size_in_kb", "1");
         test.put("training_sample_kb", "1");
         dictCompressor = ZstdCompressor.create(test);
    }

    @Test
    public void testRoundTrip() throws IOException
    {
        String example = "Some constant data with some non random data" + randomUUID();
        byte[] result = assertRoundTrip(example.getBytes(), compressor);

        assertEquals(new String(result), example);

    }

    @Test
    public void dictTraining() throws IOException
    {

        String preDictionaryText = "This is a random text with random values " + randomUUID();
        byte[] bytes = preDictionaryText.getBytes();


        ByteBuffer compressionInput = bytesToDirectBuffer(bytes);
        ByteBuffer compressionOutput = ByteBuffer.allocateDirect(compressor.initialCompressedBufferLength(bytes.length));

        dictCompressor.compress(compressionInput, compressionOutput);
        byte[] compressedBytes = directBufferToBytes(compressionOutput, true);

        ByteBuffer decompressionInput = bytesToDirectBuffer(compressedBytes);
        ByteBuffer decompressionOutput = ByteBuffer.allocateDirect(bytes.length);

        String text;
        while (dictCompressor.getDictionary() == null)
        {
            text = "This is a random text with random values " + randomUUID();
            dictCompressor.maybeSample(ByteBuffer.wrap(text.getBytes()));
        }

        dictCompressor.uncompress(decompressionInput, decompressionOutput);
        byte[] decompressedBytes = directBufferToBytes(decompressionOutput, true);

        String decompressedText = new String(decompressedBytes);

        assertEquals(decompressedText, preDictionaryText);

        assertEquals(compressionInput.position(), compressionInput.limit());
        assertEquals(decompressionInput.position(), decompressionInput.limit());


        String example = "Some constant data with some non random data" + randomUUID();
        assertRoundTrip(example.getBytes(), dictCompressor);
    }

    private ByteBuffer bytesToDirectBuffer(byte[] inputBytes) {
        return (ByteBuffer) ByteBuffer.allocateDirect(inputBytes.length).put(inputBytes).flip();
    }

    private byte[] directBufferToBytes(ByteBuffer inputBuffer, boolean flip) {
        inputBuffer.flip();
        byte[] output = new byte[inputBuffer.remaining()];
        inputBuffer.get(output);
        return output;
    }

    private byte[] assertRoundTrip(byte[] input, ICompressor compressor) throws IOException
    {
        ByteBuffer compressionInput = bytesToDirectBuffer(input);
        ByteBuffer compressionOutput = ByteBuffer.allocateDirect(compressor.initialCompressedBufferLength(input.length));

        compressor.compress(compressionInput, compressionOutput);

        byte[] compressedBytes = directBufferToBytes(compressionOutput, true);
        ByteBuffer decompressionInput = bytesToDirectBuffer(compressedBytes);
        ByteBuffer decompressionOutput = ByteBuffer.allocateDirect(input.length);

        compressor.uncompress(decompressionInput, decompressionOutput);

        byte[] decompressedBytes = directBufferToBytes(decompressionOutput, true);

        assertArrayEquals(decompressedBytes, input);
        return decompressedBytes;
    }
}