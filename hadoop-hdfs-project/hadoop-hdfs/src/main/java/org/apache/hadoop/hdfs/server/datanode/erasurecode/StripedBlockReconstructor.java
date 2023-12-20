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
package org.apache.hadoop.hdfs.server.datanode.erasurecode;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.server.datanode.DataNodeFaultInjector;
import org.apache.hadoop.hdfs.server.datanode.metrics.DataNodeMetrics;
import org.apache.hadoop.io.erasurecode.coder.util.tracerepair.RecoveryTable;
import org.apache.hadoop.io.erasurecode.rawcoder.InvalidDecodingException;
import org.apache.hadoop.util.Time;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * StripedBlockReconstructor reconstruct one or more missed striped block in
 * the striped block group, the minimum number of live striped blocks should
 * be no less than data block number.
 */
@InterfaceAudience.Private
class StripedBlockReconstructor extends StripedReconstructor
        implements Runnable {
  private final RecoveryTable recoveryTable = new RecoveryTable();

  private final int nodeCount = getStripedReader().numberOfInputs();
  ByteBuffer[] totalByteBuffers = new ByteBuffer[nodeCount];
  private StripedWriter stripedWriter;

  private ReconstructTargetInputs reconstructTargetInputs;

  StripedBlockReconstructor(ErasureCodingWorker worker,
                            StripedReconstructionInfo stripedReconInfo) {
    super(worker, stripedReconInfo);

    stripedWriter = new StripedWriter(this, getDatanode(),
            getConf(), stripedReconInfo);
  }

  boolean hasValidTargets() {
    return stripedWriter.hasValidTargets();
  }

  @Override
  public void run() {
    try {
      initDecoderIfNecessary();
      initDecodingValidatorIfNecessary();
      getStripedReader().init();
      stripedWriter.init();
      reconstruct();
      stripedWriter.endTargetBlocks();
      // Currently we don't check the acks for packets, this is similar as
      // block replication.
    } catch (Throwable e) {
      LOG.warn("Failed to reconstruct striped block: {}", getBlockGroup(), e);
      getDatanode().getMetrics().incrECFailedReconstructionTasks();
    } finally {
      float xmitWeight = getErasureCodingWorker().getXmitWeight();
      // if the xmits is smaller than 1, the xmitsSubmitted should be set to 1
      // because if it set to zero, we cannot to measure the xmits submitted
      int xmitsSubmitted = Math.max((int) (getXmits() * xmitWeight), 1);
      getDatanode().decrementXmitsInProgress(xmitsSubmitted);
      final DataNodeMetrics metrics = getDatanode().getMetrics();
      metrics.incrECReconstructionTasks();
      metrics.incrECReconstructionBytesRead(getBytesRead());
      metrics.incrECReconstructionRemoteBytesRead(getRemoteBytesRead());
      metrics.incrECReconstructionBytesWritten(getBytesWritten());

      getStripedReader().close();
      stripedWriter.close();
      cleanup();
    }
  }

  @Override
  void reconstruct() throws IOException {
    int erasedNodeIndex = getStripedReader().getErasedIndex();
    reconstructTargetInputs =
        new ReconstructTargetInputs(nodeCount, getMaxTargetLength(), erasedNodeIndex, recoveryTable);
    while (getPositionInBlock() < getMaxTargetLength()) {
      DataNodeFaultInjector.get().stripedBlockReconstruction();
      long remaining = getMaxTargetLength() - getPositionInBlock();
      final int toReconstructLen =
              (int) Math.min(getStripedReader().getBufferSize(), remaining);

      long start = Time.monotonicNow();
      long bytesToRead = (long) toReconstructLen * getStripedReader().getMinRequiredSources();
      if (getDatanode().getEcReconstuctReadThrottler() != null) {
        getDatanode().getEcReconstuctReadThrottler().throttle(bytesToRead);
      }
      // step1: read from minimum source DNs required for reconstruction.
      // The returned success list is the source DNs we do real read from
      getStripedReader().readMinimumSources(toReconstructLen);
      long readEnd = Time.monotonicNow();

      // step2: decode to reconstruct targets
      // mergeData(toReconstructLen);
      reconstructTargets(toReconstructLen);
      long decodeEnd = Time.monotonicNow();

      // step3: transfer data
      long bytesToWrite = (long) toReconstructLen * stripedWriter.getTargets();
      if (getDatanode().getEcReconstuctWriteThrottler() != null) {
        getDatanode().getEcReconstuctWriteThrottler().throttle(bytesToWrite);
      }
      if (stripedWriter.transferData2Targets() == 0) {
        String error = "Transfer failed for all targets.";
        throw new IOException(error);
      }
      long writeEnd = Time.monotonicNow();

      // Only successful reconstructions are recorded.
      final DataNodeMetrics metrics = getDatanode().getMetrics();
      metrics.incrECReconstructionReadTime(readEnd - start);
      metrics.incrECReconstructionDecodingTime(decodeEnd - readEnd);
      metrics.incrECReconstructionWriteTime(writeEnd - decodeEnd);
      updatePositionInBlock(toReconstructLen);
      clearBuffers();
    }
    // reconstructTargets();
  }

  public static ByteBuffer concat(ByteBuffer[] buffers, int overallCapacity) {
    // ByteBuffer all = ByteBuffer.allocateDirect(overallCapacity);
    ByteBuffer all = ByteBuffer.allocate(overallCapacity);
    for (ByteBuffer buffer : buffers) {
      ByteBuffer curr = buffer.slice();
      all.put(curr);
    }
    all.rewind();
    return all;
  }

  public static ByteBuffer clone(ByteBuffer original) {
    ByteBuffer clone = ByteBuffer.allocate(original.capacity());
    original.rewind();  // Copy from the beginning.
    clone.put(original);
    original.rewind();
    clone.flip();
    return clone;
  }

  private void updateByteBuffers(ByteBuffer [] inputs) {
    for (int i = 0; i < inputs.length; i++) {
      ByteBuffer receivedByteBuffer = totalByteBuffers[i];
      ByteBuffer newInput = inputs[i];
      if (newInput == null) {
        continue;
      }
      if (receivedByteBuffer != null) {
        ByteBuffer[] temps = new ByteBuffer[] {receivedByteBuffer, newInput};
        int totalCapacity = receivedByteBuffer.capacity() + newInput.capacity();
        totalByteBuffers[i] = concat(temps, totalCapacity);
      } else {
        totalByteBuffers[i] = clone(newInput);
      }
    }
  }

  private void mergeData(int toReconstructLen) throws IOException {
    ByteBuffer[] inputs = getStripedReader().getInputBuffers(toReconstructLen);
    updateByteBuffers(inputs);
  }

  ByteBuffer[] correct = null;
  ByteBuffer[] current = null;

  private void reconstructTargets(int toReconstructLen) throws IOException {
    ByteBuffer[] inputs = getStripedReader().getInputBuffers(toReconstructLen);
    reconstructTargetInputs.appendInputs(inputs, toReconstructLen);
    ByteBuffer[] decoderInputs = reconstructTargetInputs.getInputs();

    int[] erasedIndices = stripedWriter.getRealTargetIndices();
    ByteBuffer[] outputs = stripedWriter.getRealTargetBuffers(toReconstructLen);

    if (correct == null) {  // [DEBUG]
      correct = decoderInputs;
    }
    current = decoderInputs;

    for (int i = 0; i < correct.length; i++) {  // [DEBUG]
      if (correct[i] == null) {
        continue;
      }
      boolean same = Arrays.equals(correct[i].array(), current[i].array());
      System.out.println(same);
    }

    if (isValidationEnabled()) {
      markBuffers(inputs);
      decode(decoderInputs, erasedIndices, outputs);
      resetBuffers(inputs);

      DataNodeFaultInjector.get().badDecoding(outputs);
      long start = Time.monotonicNow();
      try {
        getValidator().validate(inputs, erasedIndices, outputs);
        long validateEnd = Time.monotonicNow();
        getDatanode().getMetrics().incrECReconstructionValidateTime(
                validateEnd - start);
      } catch (InvalidDecodingException e) {
        long validateFailedEnd = Time.monotonicNow();
        getDatanode().getMetrics().incrECReconstructionValidateTime(
                validateFailedEnd - start);
        getDatanode().getMetrics().incrECInvalidReconstructionTasks();
        throw e;
      }
    } else {
      /*assert erasedIndices.length == 1;
      byte[] output = outputs[0].array();
      Arrays.fill(output, (byte) 78);*/  // [DEBUG]
      decode(decoderInputs, erasedIndices, outputs);
    }

    stripedWriter.updateRealTargetBuffers(toReconstructLen);
    reconstructTargetInputs.forwardInputs();
  }

  /*private void reconstructTargets() throws IOException {
    long remaining = getMaxTargetLength() - getPositionInBlock();
    final int toReconstructLen =
            (int) Math.min(getStripedReader().getBufferSize(), remaining);

    int reconstructLen = receivedByteBuffers[0].capacity();
    // [DEBUG] Read up to input length of data for each input array.
    int erasedNodeIndex = getStripedReader().getErasedIndex();
    ByteBuffer[] decoderInputs = new ByteBuffer[getStripedReader().numberOfInputs()];

    // int decodeLength = (int) (reconstructLen * (1 / 8.0) / 4.0);   // [FIXME] Allocate the correct size.
    // ByteBuffer[] decoderOutputs = new ByteBuffer[1];
    ByteBuffer[] decoderOutputs = stripedWriter.getRealTargetBuffers(toReconstructLen);
    // ByteBuffer[] decoderOutputs = stripedWriter.getRealTargetBuffers((int) getMaxTargetLength());
    // decoderOutputs[0] = ByteBuffer.allocate(decodeLength);
    for (int nodeIndex = 0; nodeIndex < receivedByteBuffers.length; nodeIndex++) {
      if (nodeIndex == erasedNodeIndex) { continue; }
      int bandwidth = recoveryTable.getByte_9_6(nodeIndex, erasedNodeIndex, 0);
      int inputLimit = reconstructLen * bandwidth / 8;
      receivedByteBuffers[nodeIndex].position(0);
      byte[] trimmedInput = new byte[inputLimit];
      receivedByteBuffers[nodeIndex].get(trimmedInput, 0, inputLimit);
      decoderInputs[nodeIndex] = ByteBuffer.wrap(trimmedInput);
    }
    decode(decoderInputs, new int[]{erasedNodeIndex}, decoderOutputs);
    stripedWriter.updateRealTargetBuffers(toReconstructLen);
  }*/

  private void decode(ByteBuffer[] inputs, int[] erasedIndices,
                      ByteBuffer[] outputs) throws IOException {
    long start = System.nanoTime();
    getDecoder().decode(inputs, erasedIndices, outputs);
    long end = System.nanoTime();
    this.getDatanode().getMetrics().incrECDecodingTime(end - start);
  }

  /**
   * Clear all associated buffers.
   */
  private void clearBuffers() {
    getStripedReader().clearBuffers();
    stripedWriter.clearBuffers();
  }

  static class ReconstructTargetInputs {
    ByteBuffer[] inputs;
    byte[] bandwidth;
    int nodeCount;
    int erasedNodeIndex;
    RecoveryTable recoveryTable;
    int[] bufferPointers;
    byte[][] totalInputs;

    ReconstructTargetInputs(int nodeCount, long maxTargetLength, int erasedNodeIndex, RecoveryTable recoveryTable) {
      this.inputs = new ByteBuffer[nodeCount];
      this.totalInputs = new byte[nodeCount][(int) maxTargetLength];
      this.nodeCount = nodeCount;
      this.erasedNodeIndex = erasedNodeIndex;
      this.recoveryTable = recoveryTable;
      this.bufferPointers = new int[nodeCount];
      this.bandwidth = new byte[nodeCount];
      for (int nodeIndex = 0; nodeIndex < nodeCount; nodeIndex++) {
        bandwidth[nodeIndex] = recoveryTable.getByte_9_6(nodeIndex, erasedNodeIndex, 0);
      }
    }

    public static ByteBuffer clone(ByteBuffer original) {
      ByteBuffer clone = ByteBuffer.allocate(original.capacity());
      original.rewind();  // Copy from the beginning.
      clone.put(original);
      original.rewind();
      clone.flip();
      return clone;
    }

    public static ByteBuffer concatenate(ByteBuffer[] buffers, int capacity) {
      ByteBuffer out = ByteBuffer.allocate(capacity);
      for (ByteBuffer buffer : buffers) {
        ByteBuffer curr = buffer.slice();
        out.put(curr);
      }
      out.rewind();
      return out;
    }

    ByteBuffer[] correct = null;  // [DEBUG]

    void appendInputs(ByteBuffer[] receivedByteBuffers, int toReconstructLen) {
      assert inputs.length == receivedByteBuffers.length;

      // [DEBUG]
      if (correct == null) {
        correct = receivedByteBuffers;
      }
      for (int i = 0; i < receivedByteBuffers.length; i++) {
        if (receivedByteBuffers[i] == null) {
          continue;
        }
        boolean same = Arrays.equals(correct[i].array(), receivedByteBuffers[i].array());
        System.out.println(same);
        assert same;
      }


      for (int nodeIndex = 0; nodeIndex < nodeCount; nodeIndex++) {
        if (nodeIndex == erasedNodeIndex) { continue; }
        assert receivedByteBuffers[nodeIndex] != null;
        int originalPosition = receivedByteBuffers[nodeIndex].position();

        receivedByteBuffers[nodeIndex].get(totalInputs[nodeIndex], bufferPointers[nodeIndex], toReconstructLen);

        /*if (inputs[nodeIndex] == null) {
          inputs[nodeIndex] = clone(receivedByteBuffers[nodeIndex]);
        } else {
          ByteBuffer[] temps = new ByteBuffer[]{inputs[nodeIndex], receivedByteBuffers[nodeIndex]};
          int totalCapacity = inputs[nodeIndex].capacity() + receivedByteBuffers[nodeIndex].capacity();
          inputs[nodeIndex] = concat(temps, totalCapacity);
        }*/
        /*receivedByteBuffers[nodeIndex].position(
          originalPosition + (toReconstructLen * bandwidth[nodeIndex] / 8));*/
      }
    }

    ByteBuffer[] getInputs() {
      ByteBuffer[] decoderInputs = new ByteBuffer[nodeCount];
      for (int nodeIndex = 0; nodeIndex < inputs.length; nodeIndex++) {
        if (nodeIndex == erasedNodeIndex) { continue; }
        int bandwidth = recoveryTable.getByte_9_6(nodeIndex, erasedNodeIndex, 0);
        int inputLimit = 32768 * bandwidth / 8;

        byte[] trimmedInput = new byte[inputLimit];
        System.arraycopy(totalInputs[nodeIndex], bufferPointers[nodeIndex], trimmedInput, 0, inputLimit);
        decoderInputs[nodeIndex] = ByteBuffer.wrap(trimmedInput);
        /*byte[] trimmedInput = new byte[inputLimit];
        // inputs[nodeIndex].position(bufferPointers[nodeIndex]);
        inputs[nodeIndex].rewind();
        inputs[nodeIndex].get(trimmedInput, 0, inputLimit);
        decoderInputs[nodeIndex] = ByteBuffer.wrap(trimmedInput);*/
      }
      return decoderInputs;
    }

    void forwardInputs() {
      for (int nodeIndex = 0; nodeIndex < inputs.length; nodeIndex++) {
        if (nodeIndex == erasedNodeIndex) { continue; }
        int bandwidth = recoveryTable.getByte_9_6(nodeIndex, erasedNodeIndex, 0);
        bufferPointers[nodeIndex] = bufferPointers[nodeIndex] + 32768 * bandwidth / 8;
      }
      /*for (int nodeIndex = 0; nodeIndex < inputs.length; nodeIndex++) {
        if (nodeIndex == erasedNodeIndex) { continue; }
        ByteBuffer nodeBuffer = inputs[nodeIndex];
        assert nodeBuffer != null;
        int nodeBandwidth = bandwidth[nodeIndex];
        int readAlready = nodeBuffer.position() + (inputs[nodeIndex].capacity() * nodeBandwidth / 8);
        nodeBuffer.position(readAlready);
      }*/
    }
  }
}
