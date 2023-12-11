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
import org.apache.hadoop.io.erasurecode.coder.util.tracerepair.RecoveryTable96String;
import org.apache.hadoop.io.erasurecode.coder.util.tracerepair.DualBasisTable96String;
import org.apache.hadoop.io.erasurecode.coder.util.tracerepair.RecoveryTable;
import org.apache.hadoop.io.erasurecode.coder.util.tracerepair.DualBasisTable;
import java.nio.ByteBuffer;
import java.util.*;

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

    // [FIXME] Remove.
    /** to access entries of the Recovery table */
    private final RecoveryTable96String RTable96 = new RecoveryTable96String();

    // [FIXME] Remove.
    /** to access entries of the Dual basis table */
    private final DualBasisTable96String DTable96 = new DualBasisTable96String();

    private final RecoveryTable recoveryTable = new RecoveryTable();

    private final DualBasisTable dualBasisTable = new DualBasisTable();

    private byte[] preComputedParity = new byte[256];

    private byte[] bw = new byte[9];

    /** Number of target traces.
     *  Original comment: length
     */
    int t = 8;  // Same as l.

    /** to store column traces from the helper nodes */
    HashMap<Integer, ArrayList<Boolean>> columnTraces = new HashMap<>();

    /** boolean array to store the 't' target traces */
    boolean[] targetTraces = new boolean[t];

    @Override
    protected void doDecode(ByteBufferDecodingState decodingState) {
        CoderUtil.resetOutputBuffers(decodingState.outputs,
                decodingState.decodeLength);
        throw new RuntimeException("Not tested yet.");  // [DEBUG]

        /*ByteBuffer output=decodingState.outputs[0];
        ourlog.write("In ByteBufferDecodingState ....decodingState.outputs[0] length: "+output.capacity());
        int erasedIdx=decodingState.erasedIndexes[0];

        ByteBuffer[] inputs = new ByteBuffer[] { convertListBytes(dataBytes0()),
                convertListBytes(dataBytes1()),
                convertListBytes(dataBytes2()),
                convertListBytes(dataBytes3()),
                convertListBytes(dataBytes4()),
                convertListBytes(dataBytes5()),
                convertListBytes(dataBytes6()),
                convertListBytes(dataBytes7()),
                convertListBytes(dataBytes8()) };
        // ourlog.write("Helpers: "+(decodingState.inputs.length-1));
        // ourlog.write("Erased Index: "+erasedIdx);
        // column trace computation for the trace repair process
        prepareColumnTracesByteBuffer(inputs, erasedIdx);

        // Compute 't' target traces from the column traces
        // And recover the lost data and write to output buffer
        computeTargetTracesAndRecoverByteBuffer(inputs, erasedIdx, output);*/
    }

    @Override
    protected void doDecode(ByteArrayDecodingState decodingState) {
        // [DEBUG] Standard decode goes in here.
        byte[] output = decodingState.outputs[0];
        CoderUtil.resetOutputBuffers(
            decodingState.outputs,
            decodingState.outputOffsets,
            decodingState.decodeLength
        );
        // [FIXME] This should be done over every erased node.
        int erasedIdx = decodingState.erasedIndexes[0];
        /*ByteBuffer[] inputs = new ByteBuffer[] {
            convertListBytes(dataBytes0()),
            convertListBytes(dataBytes1()),
            convertListBytes(dataBytes2()),
            convertListBytes(dataBytes3()),
            convertListBytes(dataBytes4()),
            convertListBytes(dataBytes5()),
            convertListBytes(dataBytes6()),
            // convertListBytes(dataBytes7()),
            convertListBytes(dataBytes8())
        };*/
        int testLen = 10;
        int n = decodingState.decoder.getNumDataUnits();
        assert(n == 6);
        // [NOTE] bw would be required for getting the decimal traces.
        for (int i = 0; i < n; i++) {
            bw[i] = recoveryTable.getByte_9_6(i, erasedIdx, 0);
        }
        // [TODO] Convert traces to bytes.
        byte[] revMem = repairDecimalTrace(n, erasedIdx);
        byte[] rev = constructCj(erasedIdx, testLen, revMem, decodingState);
        String s = Arrays.toString(rev);
        // [TODO] Write rev into the output buffer.
        /*ourlog.write(this, "received data length: " + inputs.length);
        ourlog.write(this, "received data " + 0 + ": " + Arrays.toString(Arrays.copyOfRange(inputs[0].array(), 0, 30)));

        for (int i = 0; i < inputs.length; i++) {
            // if (i == erasedIdx) {
                continue;
            }
            ByteBuffer tempInput = inputs[i];
            byte[] tempByteArray = new byte[tempInput.limit()];
            tempInput.get(tempByteArray);
            ourlog.write(this, "received data - index: " + i + ": " + Arrays.toString(Arrays.copyOfRange(tempByteArray, 0, 30)));
        }*/
        /*// Column trace computation for the trace repair process
        prepareColumnTracesByteBuffer(inputs, erasedIdx);
        // Compute 't' target traces from the column traces
        // And recover the lost data and write to output buffer
        ByteBuffer bufOutput = ByteBuffer.allocate(output.length);
        bufOutput.put(output, 0, output.length);
        bufOutput.position(0);
        computeTargetTracesAndRecoverByteBuffer(inputs, erasedIdx, bufOutput);*/
    }

    private byte[] constructCj(
        int erasedIdx,
        int testLen,
        byte[] revMem,
        ByteArrayDecodingState decodingState
    ) {
        // [FIXME] Adapt to use a ByteBuffer instead.
        byte[][] debugTrace = {
            { 1, 1, 1, 1, 1, 1, 1, 1, 1, 1 },
            { 45, 45, 45, 45, 45, 45, 45, 45, 45, 45 },
            { 9, 9, 9, 9, 9, 9, 9, 9, 9, 9 },
            { 2, 2, 2, 2, 2, 2, 2, 2, 2, 2 },
            { 26, 26, 26, 26, 26, 26, 26, 26, 26, 26 },
            { 44, 44, 44, 44, 44, 44, 44, 44, 44, 44 },
            { 10, 10, 10, 10, 10, 10, 10, 10, 10, 10 },
            { 11, 11, 11, 11, 11, 11, 11, 11, 11, 11 },
        };

        byte[] rev = new byte[testLen];
        // [NOTE] Traces (inputs) should not involve any erased nodes.
        for (int i = 0; i < debugTrace.length; i++) {
            // [NOTE] The traces should not include the erased nodes.
            // if (i == erasedIdx) { continue; }
            for (int test_codeword = 0; test_codeword < testLen; test_codeword++) {
                byte traces_as_number = debugTrace[i][test_codeword];
                // [FIXME] Handle multi-node failures.
                int dataNodeIdx = erasedIdx <= i ? i + 1 : i;
                rev[test_codeword] = (byte) (rev[test_codeword] ^ revMem[(dataNodeIdx << 8) + traces_as_number]);;
            }
        }
        return rev;
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

    private void preCompute() {
        int i;
        preComputedParity[0] = 0;
        for (i = 1; i < 256; i++) {
            preComputedParity[i] = (byte) (preComputedParity[i >> 1] ^ (i & 1));
        }
    }

    /**
     * Perform trace repair from the ByteBuffer traces received and recovered
     * from the lost block into outputs.
     * @param inputs Input buffers of the helper nodes to read the data from.
     * @param erasedIdx Indexes of erased unit in the inputs array.
     */
    protected void prepareColumnTracesByteBuffer(ByteBuffer[] inputs, int erasedIdx) {
        int helperNodes = inputs.length;
        for (
                int helperNodeIndex = 0, k = 0;
                helperNodeIndex < helperNodes;
                helperNodeIndex++, k++
        ) {
            if (helperNodeIndex == erasedIdx) { continue; }
            // [NOTE] The first entry (bandwidth) is not ignored here.
            Object element = RTable96.getElement(helperNodeIndex, erasedIdx);
            String[] elements = element.toString().split(",");
            int bandwidth = Integer.parseInt(elements[0]);
            // [NOTE] Get helper trace elements from helper
            //        helperNodeIndex's.
            //        Inputs byte buffer into a byte array.
            ByteBuffer _helperTrace = inputs[helperNodeIndex];
            // [INFO] Sets limit to the current write position.
            _helperTrace.flip();
            int n = _helperTrace.limit();
            // [WARN] We need to confirm this.
            //        Original comment: Already done by flip I think.
            _helperTrace.rewind();
            byte[] helperTrace = new byte[n];
            _helperTrace.get(helperTrace);
            // [WARN] 5120 is the number of data bytes
            //        in a packet (excluding header,
            //        TR ignores checksum). This is a safe
            //        assumption for now.
            int originalBytesInInput = 5120;
            // [NOTE] Create a boolean array containing all
            //        trace bits in the input byte array by
            //        calling convertByteToBoolean().
            //        The 2nd argument specifies the total
            //        number of trace bits (#bytes in the
            //        buffer * bandwidth) in this input
            //        buffer.
            boolean[] helperTraceBool = convertByteToBoolean(
                helperTrace,
                originalBytesInInput*bandwidth
            );
            // [NOTE] Store column traces from this helper
            //        node.
            ArrayList<Boolean> columnTraceStorage = new ArrayList<>();
            // [NOTE] We need to process only bandwidth
            //        bits at a time from helper trace data.
            for (
                    int bitIndex = 0;
                    bitIndex < helperTraceBool.length;
                    bitIndex += bandwidth
            ) {
                // [INFO] Slice a new boolean array composed of
                //        bits from helperTraceBool from fromIndex
                //        (inclusive) to toIndex (exclusive).
                // [NOTE] Bit index is incremented by the bandwidth.
                boolean[] helperTraceBits = Arrays.copyOfRange(
                    helperTraceBool, bitIndex,
                    bitIndex + bandwidth
                );
                // [INFO] Store the trace bits into an ArrayList.
                ArrayList<Boolean> helperTraceArray = new ArrayList<>();
                for (boolean helperTraceBit : helperTraceBits) helperTraceArray.add(helperTraceBit);
                // [INFO] Store helperTrace in a Vector.
                Vector<Boolean> helperTraceVector = new Vector<>(helperTraceArray);
                // [DEBUG] t is 8.
                for (int s = 1; s <= t; s++) {
                    // [NOTE] This should skip the first entry (bandwidth).
                    String repairString = elements[s];
                    int repairInt = Integer.parseInt(repairString.trim());
                    boolean[] _repairBinaryExpanded = binaryRepresentation(bandwidth, repairInt);
                    // [INFO] Store the _repairBinaryExpanded representation as a Vector.
                    Vector<Boolean> repairBinaryExpanded = new Vector<>(_repairBinaryExpanded.length);
                    for (boolean value : _repairBinaryExpanded) {
                        repairBinaryExpanded.add(value);
                    }
                    // Boolean array to store bit-wise and of binaryRep and helperTrace.
                    boolean[] result = new boolean[bandwidth];
                    // Computing column traces from this set of trace bits
                    for (int l = 0; l < bandwidth; l++) {
                        boolean a = Boolean.TRUE.equals(repairBinaryExpanded.get(l));
                        boolean b = Boolean.TRUE.equals(helperTraceVector.get(l));
                        result[l] = a & b;
                    }
                    // Boolean to compute the XOR of all bits of result.
                    boolean output = false;
                    for (boolean bit : result) {
                        output ^= bit;
                    }
                    // ArrayList to store the output bit.
                    columnTraceStorage.add(output);
                }
            }
            // Store this as the column trace of helper node k.
            columnTraces.put(k, columnTraceStorage);
        }
    }

    /*protected void prepareColumnTracesByteBuffer(ByteBuffer[] inputs, int erasedIdx) {
        int k = 0;
        for (int i = 0; i < inputs.length; i++) {  // Iterate through all helpers
            if (i == erasedIdx) {
                continue;
            }
            // [DEBUG] Does this ignore the first entry (bandwidth)?
            Object element = RTable96.getElement(i, erasedIdx);
            String st = element.toString();
            String[] elements = st.split(",");
            int traceBandwidth = Integer.parseInt(elements[0]);
            // Get helper trace elements from helper i's inputs byte buffer into a byte array.
            ByteBuffer helperTraceByteBuffer = inputs[i];
            // byte[] inputArray = new byte[helperTraceByteBuffer.remaining()];
            // helperTraceByteBuffer.get(inputArray);
            // ourlog.write(this, "do decode - index: " + i + " - inputArray: " + inputArray.length);
            // ourlog.write(this, "do decode - index: " + i + " - inputArray: " + Arrays.toString(Arrays.copyOfRange(inputArray, 0, 30)));
            helperTraceByteBuffer.flip();  // Sets limit to the current write position.
            int n = helperTraceByteBuffer.limit();

            helperTraceByteBuffer.rewind(); // [WARN] We need to confirm this. Original comment: Already done by flip I think.
            byte[] helperTraceByteArray = new byte[n];
            helperTraceByteBuffer.get(helperTraceByteArray);
            // ourlog.write(this, "do decode - index: " + i + " - helperTraceByteArray: " + helperTraceByteArray.length);
            // ourlog.write(this, "do decode - index: " + i + " - helperTraceByteArray: " + Arrays.toString(Arrays.copyOfRange(helperTraceByteArray, 0, 30)));


            // [SAFE ASSUMPTIONS] 5120 is the number of data bytes in a packet (excluding header, TR ignores checksum)
            int originalBytesInInput = 5120;

            // Create a boolean array containing all the trace bits in the input byte array by calling convertByteToBoolean().
            // The 2nd argument specifies the the total number of trace bits (#bytes in the buffer * traceBandwidth) in this input buffer
            boolean[] helperTraceBooleanArray = convertByteToBoolean(helperTraceByteArray, originalBytesInInput*traceBandwidth);
            // ourlog.write(this, "do decode - index: " + i + " - helperTraceBooleanArray: " + helperTraceBooleanArray.length);
            // ourlog.write(this, "do decode - index: " + i + " - helperTraceBooleanArray: " + Arrays.toString(Arrays.copyOfRange(helperTraceBooleanArray, 0, 30)));

            // boolean arraylist to store the column traces from this helper node
            ArrayList<Boolean> ar=new ArrayList<>();

            // We need to process only traceBandwidth bits at a time from helper trace data
            for (int traceBitsIndex=0; traceBitsIndex < helperTraceBooleanArray.length; traceBitsIndex=traceBitsIndex + traceBandwidth) {
                // Returns a new boolean array composed of bits from helperTraceBooleanArray from fromIndex (inclusive) to toIndex (exclusive).
                boolean[] helperTraceBits=Arrays.copyOfRange(helperTraceBooleanArray, traceBitsIndex, traceBitsIndex + traceBandwidth);

                // Store the trace bits into an ArrayList.
                ArrayList<Boolean> helperTraceArray=new ArrayList<>();
                for (int h=0; h < helperTraceBits.length; h++)
                    helperTraceArray.add(helperTraceBits[h]);

                // Get helper trace elements computed into a Vector.
                Vector<Boolean> helperTraceVector = new Vector<Boolean>(helperTraceArray);

                for (int s=1; s <= t; s++) {
                    String repairString=elements[s].toString();
                    Integer repairInt=Integer.parseInt(repairString.trim());
                    boolean bin[]=binaryRep(traceBandwidth, repairInt);

                    //Store the binary rep as a Vector
                    Vector<Boolean> binVec=new Vector<Boolean>(bin.length);
                    for (int m=0; m < bin.length; m++) {
                        // System.out.println(bin[m]);
                        binVec.add(bin[m]);
                    }

                    // Boolean array to store the bit-wise & of binRep and helperTrace
                    boolean[] res = new boolean[traceBandwidth];

                    // Computing column traces from this set of trace bits
                    for (int l = 0; l < traceBandwidth; l++) {
                        boolean a = Boolean.TRUE.equals(binVec.get(l));
                        boolean b = Boolean.TRUE.equals(helperTraceVector.get(l));
                        res[l] = a & b;

                    }
                    // boolean to compute the XOR of all bits of res.
                    boolean output=false;
                    for (int l = 0; l < res.length; l++) {
                        output ^= res[l];
                    }
                    // ArrayList to store the output bit.
                    ar.add(output);
                }

            }
            // Store this as the column trace of helper node k.
            columnTraces.put(k, ar);
            k++;
        }
        // ourlog.write(this, "do decode - columnTraces: " + columnTraces.size());
        // for(Integer key: columnTraces.keySet()) {
        //     if (columnTraces.get(k)  != null) {
        //         ourlog.write(this, "computeTargetTraces: " + columnTraces.get(k).subList(0, 30));
        //     }
        // }
        // System.out.println("columnTraces: " + columnTraces);
    }*/
    /**
     * Compute t target traces from the column traces
     * @param erasedIdx indexes of erased unit in the inputs array
     * @param output the buffer to store the recovered bytes of the lost block
     * */
    private void computeTargetTracesAndRecoverByteBuffer(ByteBuffer[] inputs, int erasedIdx, ByteBuffer output) {
        byte[] tempOutputBytes = output.array();
        System.out.println("output: " + output.array().length);
        //Retrieve the dual basis element and keep it converted as byte
        Object dBTableElement = DTable96.getElement(erasedIdx);
        String st = dBTableElement.toString();
        //System.out.println("Dual basis elements are: "+st);
        String[] dBTableElements = st.split(",");
        Integer[] dualBasisInt = new Integer[t];
        byte[] dualBasisByte = new byte[t];

        for(int m = 0; m < dBTableElements.length; m++){
            String dualBasisString = dBTableElements[m].toString();
            dualBasisInt[m] = Integer.parseInt(dualBasisString.trim());
            dualBasisByte[m] = dualBasisInt[m].byteValue();
        }

        for(int tr = 0, oIdx = output.position(); tr < columnTraces.size(); tr = tr + t, oIdx++) {
            for (int s = 0; s < inputs.length-1; s++) {
                for (int j = tr; j < tr + t; j++) {
                    boolean RHS = false;
                    boolean colTraceBool = columnTraces.get(j).get(s);
                    RHS ^= colTraceBool;
                    targetTraces[s] = RHS;
                }
            }
            // Now use this set of target traces to compute the byte of lost block
            byte recoveredValue=(byte) 0;
            for (int s=0; s < t; s++) {
                byte dualBByte=dualBasisByte[s]; // take the sth byte from dual basis array
                if (targetTraces[s]) {
                    recoveredValue ^= dualBByte;
                }

            }
            output.put(oIdx, recoveredValue);
        }
        // byte[]test = output.array();
        // ourlog.write(this, "computeTargetTraces: " + test.length);
        // ourlog.write(this, "computeTargetTraces: " + Arrays.toString(Arrays.copyOfRange(test, 0, 3000)));
    }

    /*private List<Byte> traceDecimalD1() {
        return Arrays.asList(1, 1, 1, 1, 1, 1, 1, 1, 1, 1);
    }

    private List<Byte> traceDecimalD2() {
        return Arrays.asList(45, 45, 45, 45, 45, 45, 45, 45, 45, 45);
    }

    private List<Byte> traceDecimalD3() {
        return Arrays.asList(9, 9, 9, 9, 9, 9, 9, 9, 9, 9);
    }

    private List<Byte> traceDecimalD4() {
        return Arrays.asList(2, 2, 2, 2, 2, 2, 2, 2, 2, 2);
    }

    private List<Byte> traceDecimalD5() {
        return Arrays.asList(26, 26, 26, 26, 26, 26, 26, 26, 26, 26);
    }

    private List<Byte> traceDecimalD6() {
        return Arrays.asList(44, 44, 44, 44, 44, 44, 44, 44, 44, 44);
    }

    private List<Byte> traceDecimalD7() {
        return Arrays.asList(10, 10, 10, 10, 10, 10, 10, 10, 10, 10);
    }

    private List<Byte> traceDecimalD8() {
        return Arrays.asList(11, 11, 11, 11, 11, 11, 11, 11, 11, 11);
    }*/

    private List<Byte> dataBytes0() {
        Byte[] defaultBytes = new Byte[] { 85, 85, 85, 85, 85, -35, -35, -35, -35, -35 };
        Byte[] endBytes = new Byte[] { 85, 85, 85, 85, 85, -35, -35, -35 };
        // int numberOfRepetition = 52428;
        int numberOfRepetition = 1;
        return dataBytesFrom(defaultBytes, endBytes, numberOfRepetition);
    }

    private List<Byte> dataBytes1() {
        Byte[] defaultBytes = new Byte[] { 85, 85, 85, 85, 85, -35, -35, -35, -35, -35 };
        Byte[] endBytes = new Byte[] { 85, 85, 85, 85, 85, -35, -35, -35 };
        // int numberOfRepetition = 52428;
        int numberOfRepetition = 1;
        return dataBytesFrom(defaultBytes, endBytes, numberOfRepetition);
    }

    private List<Byte> dataBytes2() {
        Byte[] defaultBytes = new Byte[] { -35, -35, -35, -35, -35, 85, 85, 85, 85, 85 };
        Byte[] endBytes = new Byte[] { -35, -35, -35, -35, -35, 85, 85, 85 };
        // int numberOfRepetition = 52428;
        int numberOfRepetition = 1;
        return dataBytesFrom(defaultBytes, endBytes, numberOfRepetition);
    }

    private List<Byte> dataBytes3() {
        Byte[] defaultBytes = new Byte[] { -25, -107, -41, 93, 117, -41, 93, 117, -41, -25, -98, 121, -25, -98, 121 };
        Byte[] endBytes = new Byte[] { -25, -107, -41, 93, 117, -41, 93, 117, -41, -25, -98, 121 };
        // int numberOfRepetition = 52428;
        int numberOfRepetition = 1;
        return dataBytesFrom(defaultBytes, endBytes, numberOfRepetition);
    }

    private List<Byte> dataBytes4() {
        Byte[] defaultBytes = new Byte[] { -25, -107, -41, 93, 117, -41, 93, 117, -41, -25, -98, 121, -25, -98, 121 };
        Byte[] endBytes = new Byte[] { -25, -107, -41, 93, 117, -41, 93, 117, -41, -25, -98, 121 };
        // int numberOfRepetition = 52428;
        int numberOfRepetition = 1;
        return dataBytesFrom(defaultBytes, endBytes, numberOfRepetition);
    }

    private List<Byte> dataBytes5() {
        Byte[] defaultBytes = new Byte[] { 119, 119, 119, 119, 119, 0, 0, 0, 0, 0 };
        Byte[] endBytes = new Byte[] { 119, 119, 119, 119, 119, 119, 119, 119 };
        // int numberOfRepetition = 52428;
        int numberOfRepetition = 1;
        return dataBytesFrom(defaultBytes, endBytes, numberOfRepetition);
    }

    private List<Byte> dataBytes6() {
        Byte[] defaultBytes = new Byte[] { 34, 17, 119, 34, -69, -18, -35, -69, -18, 119 };
        Byte[] endBytes = new Byte[] { 34, 17, 119, 34, -69, 85, 102, 0 };
        // int numberOfRepetition = 52428;
        int numberOfRepetition = 1;
        return dataBytesFrom(defaultBytes, endBytes, numberOfRepetition);
    }


    private List<Byte> dataBytes7() {
        Byte[] defaultBytes = new Byte[] { -98, 123, -82, 113, -50, 56, 20, 82, -53, 8, 44, 48, 81, 74, 105 };
        Byte[] endBytes = new Byte[] { -98, 123, -82, 113, -50, 56, 20, 80, 0, 36, -98, -5 };
        // int numberOfRepetition = 52428;
        int numberOfRepetition = 1;
        return dataBytesFrom(defaultBytes, endBytes, numberOfRepetition);
    }

    private List<Byte> dataBytes8() {
        Byte[] defaultBytes = new Byte[] { 51, -35, 68, 119, 0, 68, -86, 51, 0, 119 };
        Byte[] endBytes = new Byte[] { 51, -35, 68, 119, 0, -69, 85, -52 };
        // int numberOfRepetition = 52428;
        int numberOfRepetition = 1;
        return dataBytesFrom(defaultBytes, endBytes, numberOfRepetition);
    }

    /**
     * Compute the t-bit binary representation of the non-negative integer m
     * @param t the number of bits required in the representation
     * @param m the non-negative integer for which we find the t-bit representation
     * @return boolean array of the t-bits computed
     */
    public boolean[] binaryRepresentation(int t, int m){
        /*if((m < 0) || (m > Math.pow(q, t-1)))
            System.out.println("Number not in range [0..(q^t-1)]"); */
        boolean[] bin = new boolean[t];
        Arrays.fill(bin, Boolean.FALSE);

      /*  for(int i = 0; i < bin.length; i++) {
            System.out.println(bin[i]);
        } */

        while (m > 1) {
            int log = log2(m);
            int pos = (t - log)-1;
            if(pos < 0)
                pos = 0;
            bin[pos] = true;
            m = (int) (m - Math.pow(2, log));
        }
        if (m == 1)
            bin[t-1] = true;

       /* System.out.println("After binaryRep, the binary array is");
        for(int i = 0; i < bin.length; i++) {
            System.out.println(bin[i]);
        } */
        return bin;

    }

    private ByteBuffer convertListBytes(List<Byte> allBytes) {
        Byte[] arr = new Byte[allBytes.size()];
        byte[] byteArr = new byte[allBytes.size()];
        arr = allBytes.toArray(arr);
        int j=0;
        // Unboxing Byte values. (Byte[] to byte[])
        for(Byte b: arr) {
            byteArr[j++] = b.byteValue();
        }
        System.out.println("arr = " + Arrays.toString(byteArr));
        return ByteBuffer.wrap(byteArr);
    }

    private List<Byte> dataBytesFrom(Byte[] defaultBytes, Byte[] endBytes, int numberOfRepetition) {
        List<Byte> allBytes = new ArrayList<>();
        List<Byte> defaultByteList = Arrays.asList(defaultBytes);
        List<Byte> endByteList = Arrays.asList(endBytes);
        for (int i = 0; i < numberOfRepetition; i++) {
            allBytes.addAll(defaultByteList);
        }
        allBytes.addAll(endByteList);
        return allBytes;
    }

    /**
     * Convert a byte array into a boolean array
     *  @param bits a byte array of boolean values
     *  @param significantBits the number of total bits in the byte array, and
     *  therefore the length of the returned boolean array
     *  @return a boolean[] containing the same boolean values as the byte[] array
     *  adapted from https://sakai.rutgers.edu/wiki/site/e07619c5-a492-4ebe-8771-179dfe450ae4/bit-to-boolean%20conversion.html
     */
    public static boolean[] convertByteToBoolean(byte[] bits, int significantBits) {
        boolean[] retVal = new boolean[significantBits];
        int boolIndex = 0;
        for (int byteIndex = 0; byteIndex < bits.length; ++byteIndex) {
            for (int bitIndex = 7; bitIndex >= 0; --bitIndex) {
                if (boolIndex >= significantBits) {
                    return retVal;
                }

                retVal[boolIndex++] = (bits[byteIndex] >> bitIndex & 0x01) == 1 ? true
                        : false;
            }
        }
        return retVal;
    }

    public int log2(int N) {
        return (int)(Math.log(N) / Math.log(2));
    }
}
