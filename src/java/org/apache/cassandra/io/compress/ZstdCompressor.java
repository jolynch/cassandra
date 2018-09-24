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
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.luben.zstd.Zstd;
import com.github.luben.zstd.ZstdDictCompress;
import com.github.luben.zstd.ZstdDictDecompress;
import com.github.luben.zstd.ZstdDictTrainer;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.schema.CompressionParams;

/**
 * Supports streaming dictionary compression
 * reads first e.g. 10mb of data, and then once it obtains that it trains a dictionary with that
 * finally returning that to the metadata
 */
public class ZstdCompressor implements ICompressor
{
    private static final Logger logger = LoggerFactory.getLogger(ZstdCompressor.class);

    private static final int DEFAULT_COMPRESSION_LEVEL = 1;
    // Wait for 4 megabytes of data by default before training a dictionary
    // if zero disables training
    private static final int DEFAULT_SAMPLE_KB = 4096;
    public static final String ZSTD_COMPRESSION_LEVEL = "compression_level";
    public static final String ZSTD_SAMPLE_SIZE_KB = "training_sample_kb";
    public static final String ZSTD_DICT_SIZE = CompressionParams.DICTIONARY_SIZE;

    private ZstdDictTrainer dictTrainer;
    private final int compressionLevel;
    private final int sampleSizeKb;
    private final int dictionarySizeKb;
    // Held off-heap by Zstd itself
    private ZstdDictDecompress decompressDict;
    // Held on heap only during writing of an sstable
    private byte[] trainingDictionary;
    private ZstdDictCompress compressDict;

    public static ZstdCompressor create(Map<String, String> args) throws ConfigurationException
    {
        Integer compressionLevel = validateCompressionLevel(args.get(ZSTD_COMPRESSION_LEVEL));
        Integer sampleSizeKb = Integer.valueOf(args.getOrDefault(ZSTD_SAMPLE_SIZE_KB, String.valueOf(DEFAULT_SAMPLE_KB)));
        Integer dictionarySizeKb = Integer.valueOf(args.getOrDefault(ZSTD_DICT_SIZE, "0"));

        return new ZstdCompressor(compressionLevel, sampleSizeKb, dictionarySizeKb);
    }

    private ZstdCompressor(Integer compressionLevel, Integer sampleSizeKb, Integer dictionarySizeKb)
    {
        this.compressionLevel = compressionLevel;
        this.sampleSizeKb = sampleSizeKb;
        this.dictionarySizeKb = dictionarySizeKb;
    }

    public int initialCompressedBufferLength(int chunkLength)
    {
        return (int) Zstd.compressBound(chunkLength);
    }

    public int uncompress(byte[] input, int inputOffset, int inputLength, byte[] output, int outputOffset) throws IOException
    {
        // TODO: rewrite this so I don't copy it ...
        ByteBuffer inputBuffer = ByteBuffer.allocateDirect(inputLength);
        inputBuffer.put(input, inputOffset, inputLength).flip();
        ByteBuffer outputBuffer = ByteBuffer.allocateDirect(output.length - outputOffset);
        uncompress(inputBuffer, outputBuffer);

        ByteBuffer buffer = (ByteBuffer) outputBuffer.flip();
        int size = buffer.remaining();
        buffer.get(output, outputOffset, size);

        return size;
    }

    public void compress(ByteBuffer input, ByteBuffer output) throws IOException
    {
        int size;
        if (compressDict != null)
            size = Zstd.compress(output, input, compressDict);
        else
            size = Zstd.compress(output, input, compressionLevel);

        output.position(size);
    }

    public void uncompress(ByteBuffer input, ByteBuffer output) throws IOException
    {
        if (decompressDict != null)
            Zstd.decompress(output, input, decompressDict);
        else
            Zstd.decompress(output, input);

        output.position(output.limit());
        input.position(input.limit());
    }

    public BufferType preferredBufferType()
    {
        return BufferType.OFF_HEAP;
    }

    public boolean supports(BufferType bufferType)
    {
        return BufferType.OFF_HEAP == bufferType;
    }

    public Set<String> supportedOptions()
    {
        return new HashSet<>(Arrays.asList(ZSTD_COMPRESSION_LEVEL, ZSTD_SAMPLE_SIZE_KB, ZSTD_DICT_SIZE));

    }

    public boolean supportsDictionaries()
    {
        return dictionarySizeKb > 0;
    }

    public void maybeSample(ByteBuffer input)
    {
        if (compressDict != null)
            return;

        if (dictTrainer == null)
            dictTrainer = new ZstdDictTrainer(sampleSizeKb * 1024, dictionarySizeKb * 1024);


        boolean stillSampling = dictTrainer.addSample(input.duplicate().array());
        if (!stillSampling)
        {
            logger.trace("Sufficient samples for dictionary, training now");
            trainingDictionary = dictTrainer.trainSamples();
            compressDict = new ZstdDictCompress(trainingDictionary, compressionLevel);
            decompressDict = new ZstdDictDecompress(trainingDictionary);
            logger.trace("Done training dictionary");
        }
    }

    public void putDictionary(byte[] dictionary)
    {
        decompressDict = new ZstdDictDecompress(dictionary);
    }

    public byte[] getDictionary()
    {

        return trainingDictionary;
    }

    public static Integer validateCompressionLevel(String compressionLevel) throws ConfigurationException
    {
        if (compressionLevel == null)
            return DEFAULT_COMPRESSION_LEVEL;

        ConfigurationException ex = new ConfigurationException("Invalid value [" + compressionLevel + "] for parameter '"
                                                               + DEFAULT_COMPRESSION_LEVEL + "'. Value must be between 1 and 22.");

        Integer level;
        try
        {
            level = Integer.valueOf(compressionLevel);
        }
        catch (NumberFormatException e)
        {
            throw ex;
        }

        if (level < 1 || level > 22)
        {
            throw ex;
        }

        return level;
    }
}
