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
package org.apache.hadoop.io.erasurecode.rawcoder;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.io.erasurecode.ErasureCoderOptions;
import org.apache.hadoop.util.OurECLogger;
import org.apache.hadoop.io.erasurecode.coder.util.tracerepair.RecoveryTable;
import org.apache.hadoop.io.erasurecode.coder.util.tracerepair.DualBasisTable;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * A raw decoder of the Trace Repair code scheme in pure Java.
 *
 */

@InterfaceAudience.Private
public class TRRawDecoder extends RawErasureDecoder {
    private static OurECLogger ourlog = OurECLogger.getInstance();
    public TRRawDecoder(ErasureCoderOptions coderOptions) {
        super(coderOptions);
        preCompute();
    }

    private final RecoveryTable recoveryTable = new RecoveryTable();

    private final DualBasisTable dualBasisTable = new DualBasisTable();

    private byte[] preComputedParity = new byte[256];

    private byte[] bw = new byte[9];

    @Override
    public synchronized void decode(ByteBuffer[] inputs, int[] erasedIndexes,
                                    ByteBuffer[] outputs) throws IOException {
        ByteBufferDecodingState decodingState = new ByteBufferDecodingState(this,
                inputs, erasedIndexes, outputs, true);

        boolean usingDirectBuffer = decodingState.usingDirectBuffer;
        int dataLen = decodingState.decodeLength;
        if (dataLen == 0) {
            return;
        }

        int[] inputPositions = new int[inputs.length];
        for (int i = 0; i < inputPositions.length; i++) {
            if (inputs[i] != null) {
                inputPositions[i] = inputs[i].position();
            }
        }

        if (usingDirectBuffer) {
            doDecode(decodingState);
        } else {
            ByteArrayDecodingState badState = decodingState.convertToByteArrayState();
            doDecode(badState);
        }

        for (int i = 0; i < inputs.length; i++) {
            if (inputs[i] != null) {
                // dataLen bytes consumed
                inputs[i].position(inputPositions[i] + (int) Math.ceil(dataLen * bw[i] / 8.0));
                // inputs[i].position(inputPositions[i] + dataLen);
            }
        }
    }

    @Override
    protected void doDecode(ByteBufferDecodingState decodingState) {
        CoderUtil.resetOutputBuffers(decodingState.outputs,
                decodingState.decodeLength);
        throw new RuntimeException("Not tested yet.");  // [DEBUG]
    }

    @Override
    protected void doDecode(ByteArrayDecodingState decodingState) {
        CoderUtil.resetOutputBuffers(
            decodingState.outputs,
            decodingState.outputOffsets,
            decodingState.decodeLength
        );
        // [FIXME] This should be done over every erased node.
        int erasedIdx = decodingState.erasedIndexes[0];
        int n = decodingState.decoder.getNumAllUnits();

        // [WARN] We assume only one node fails.
        // int traceCount = n - decodingState.erasedIndexes.length;  // [FIXME] Erased trace is null. Used as the conditional in the decompress loop and the outer length of binaryTraces.
        byte[][] binaryTraces = new byte[n][];
        // [NOTE] Calculate bandwidth.
        for (int i = 0; i < n; i++) {
            bw[i] = recoveryTable.getByte_9_6(i, erasedIdx, 0);
        }
        // [NOTE] Decompress traces into its binary format.
        //        The erased trace is not sent over.
        for (int i = 0; i < n; i++) {
            if (i == erasedIdx) { continue; }
            // int activeNodeIndex = erasedIdx <= i ? i + 1 : i;  // [FIXME] Erased trace is null.
            binaryTraces[i] = decompressTrace(
                decodingState.inputs[i], decodingState.inputOffsets[i],
                bw[i] * decodingState.decodeLength);
        }
        byte[] decimalTrace = convertToDecimalTrace(
            binaryTraces, erasedIdx, decodingState.decodeLength, n);
        byte[] revMem = repairDecimalTrace(n, erasedIdx);
        constructCj(
            n, erasedIdx, decodingState.decodeLength, decimalTrace,
            revMem, decodingState.outputs[0], decodingState.outputOffsets[0]);  // [WARN] We assume only one node fails.
        System.out.println(1);
    }

    public static byte[] decompressTrace(byte[] compressedTrace, int inputOffset, int numBitsToRead) {
        byte[] decompressedTrace = new byte[numBitsToRead];
        for (int bitIndex = 0; bitIndex < numBitsToRead; bitIndex++) {
            int byteIndex = bitIndex / 8;
            int bitPos = bitIndex % 8;
            byte mask = (byte) (1 << (7 - bitPos));
            decompressedTrace[bitIndex] = (byte) ((compressedTrace[byteIndex + inputOffset] & mask) != 0 ? 1 : 0);
        }
        return decompressedTrace;
    }

    private byte[] convertToDecimalTrace(byte[][] traces, int erasedIdx, int decodeLength, int n) {
        byte[] decimalTrace = new byte[n * decodeLength];

        // int inputIndex = -1;
        for (int nodeIndex = 0; nodeIndex < n; nodeIndex++) {
            if (nodeIndex == erasedIdx) { continue; }
            // inputIndex++;  // [FIXME] Erased trace is null. Used to index traces.
            int idx = nodeIndex * decodeLength;
            // [NOTE] This is done because we skip the erased node nodeIndex and
            //        traces array does not hold traces of erased nodes.
            for (int test_codeword = 0; test_codeword < decodeLength; test_codeword++) {
                // [WARN] ISAL implementation has traces_as_number as a uint.
                int traces_as_number = 0;
                for (int a = 0; a < bw[nodeIndex]; a++) {
                    traces_as_number = traces_as_number << 1;
                    byte valueToXOR = traces[nodeIndex][a * decodeLength + test_codeword];
                    traces_as_number ^= valueToXOR;
                }
                decimalTrace[idx++] = (byte) traces_as_number;
            }
        }
        return decimalTrace;
    }

    protected byte[] repairDecimalTrace(int n, int erasedIdx) {
        // [NOTE] This should happen on a chunk by chunk basis.
        byte[] revMem = new byte[n * 256];
        byte[] erasedDualBasis = dualBasisTable.getRow_9_6(erasedIdx);
        for (int i = 0; i < n; i++) {
            if (i != erasedIdx) {
                byte[] R = recoveryTable.getRow_9_6(i, erasedIdx);
                byte[] Rij = new byte[R.length - 1];
                System.arraycopy(R, 1, Rij, 0, Rij.length);
                for (int b = 0; b < 256; b++) {
                    for (int a = 0; a < 8; a++) {
                        int parityIndex = Rij[a] & (byte) (b);
                        int xorResult = (preComputedParity[parityIndex]) * erasedDualBasis[a];
                        assert(xorResult >= -127 && xorResult <= 128);
                        revMem[(i<<8) + b] ^= (byte) xorResult;
                    }
                }
            }
        }
        return revMem;
    }

    private void constructCj(
        int n,
        int erasedIdx,
        int decodeLength,
        byte[] decimalTrace,
        byte[] revMem,
        byte[] output,
        int outputOffset
    ) {
        // [FIXME] Adapt to use a ByteBuffer instead.
        /*byte[][] debugTrace = {
            { 1, 1, 1, 1, 1, 1, 1, 1, 1, 1 },
            { 45, 45, 45, 45, 45, 45, 45, 45, 45, 45 },
            { 9, 9, 9, 9, 9, 9, 9, 9, 9, 9 },
            { 2, 2, 2, 2, 2, 2, 2, 2, 2, 2 },
            { 26, 26, 26, 26, 26, 26, 26, 26, 26, 26 },
            { 44, 44, 44, 44, 44, 44, 44, 44, 44, 44 },
            { 10, 10, 10, 10, 10, 10, 10, 10, 10, 10 },
            { 11, 11, 11, 11, 11, 11, 11, 11, 11, 11 },
        };*/

        byte[] rev = new byte[decodeLength];
        // [NOTE] Traces (inputs) should not involve any erased nodes.
        for (int i = 0; i < n; i++) {
            // [NOTE] The traces should not include the erased nodes.
            if (i == erasedIdx) { continue; }
            for (int test_codeword = 0; test_codeword < decodeLength; test_codeword++) {
                // [TODO] Handle multi-node failures.
                // int dataNodeIndex = erasedIdx <= i ? i + 1 : i;  // [FIXME] Erased trace is null.
                int traceIndex = i * decodeLength + test_codeword;
                byte traces_as_number = decimalTrace[traceIndex];
                // byte traces_as_number = decimalTrace[i][test_codeword];
                output[outputOffset + test_codeword]
                    = (byte) (output[outputOffset + test_codeword] ^ revMem[(i << 8) + traces_as_number]);;
            }
        }
        // return rev;
    }

    private void preCompute() {
        int i;
        preComputedParity[0] = 0;
        for (i = 1; i < 256; i++) {
            preComputedParity[i] = (byte) (preComputedParity[i >> 1] ^ (i & 1));
        }
    }
}
