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
package org.apache.hadoop.io.erasurecode.codec;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.erasurecode.ErasureCodecOptions;
import org.apache.hadoop.io.erasurecode.coder.ErasureDecoder;
import org.apache.hadoop.io.erasurecode.coder.ErasureEncoder;
import org.apache.hadoop.io.erasurecode.coder.TRErasureDecoder;
import org.apache.hadoop.io.erasurecode.coder.TRErasureEncoder;
import org.apache.hadoop.util.OurECLogger;
/**
 * A Trace-Repair erasure codec.
 */
@InterfaceAudience.Private
public class TRErasureCodec extends ErasureCodec {
    private static OurECLogger ourlog = OurECLogger.getInstance();

    public TRErasureCodec(Configuration conf, ErasureCodecOptions options) {
        super(conf, options);
    }

    @Override
    public ErasureEncoder createEncoder() {
        ourlog.write("\n Inside TRErasureCodec: TREncoder creating...");
        return new TRErasureEncoder(getCoderOptions());

    }

    @Override
    public ErasureDecoder createDecoder() {
        ourlog.write("\n Inside TRErasureCodec: TRDecoder creating...");
        return new TRErasureDecoder(getCoderOptions());
    }
}