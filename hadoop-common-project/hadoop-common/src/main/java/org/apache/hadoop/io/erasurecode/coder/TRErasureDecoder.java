/**
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
package org.apache.hadoop.io.erasurecode.coder;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.io.erasurecode.CodecUtil;
import org.apache.hadoop.io.erasurecode.ECBlock;
import org.apache.hadoop.io.erasurecode.ECBlockGroup;
import org.apache.hadoop.io.erasurecode.ErasureCodeConstants;
import org.apache.hadoop.io.erasurecode.ErasureCoderOptions;
import org.apache.hadoop.io.erasurecode.rawcoder.RawErasureDecoder;
import org.apache.hadoop.util.OurECLogger;

/**
 * Trace-Repair erasure decoder that decodes a block group.
 *
 * It implements {@link ErasureCoder}.
 */
@InterfaceAudience.Private
public class TRErasureDecoder extends ErasureDecoder {
    private RawErasureDecoder trRawDecoder;
    private static OurECLogger ourlog = OurECLogger.getInstance();

    public TRErasureDecoder(ErasureCoderOptions options) {
        super(options);
    }

    @Override
    protected ErasureCodingStep prepareDecodingStep(final ECBlockGroup blockGroup) {
        ECBlock[] inputBlocks = getInputBlocks(blockGroup);
        ECBlock[] outputBlocks = getOutputBlocks(blockGroup);
        RawErasureDecoder rawDecoder = checkCreateTRRawDecoder();
        return new ErasureDecodingStep(
            inputBlocks,
            getErasedIndexes(inputBlocks),
            outputBlocks,
            rawDecoder
        );
    }

    private RawErasureDecoder checkCreateTRRawDecoder() {
        if (trRawDecoder == null) {
            trRawDecoder = CodecUtil.createRawDecoder(getConf(),
                    ErasureCodeConstants.TR_CODEC_NAME, getOptions());
        }
        return trRawDecoder;
    }

    @Override
    public void release() {
        if (trRawDecoder != null) {
            trRawDecoder.release();
        }
    }
}
