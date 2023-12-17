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
import org.apache.hadoop.util.OurECLogger;
import org.apache.hadoop.util.Time;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * StripedBlockReconstructor reconstruct one or more missed striped block in
 * the striped block group, the minimum number of live striped blocks should
 * be no less than data block number.
 */
@InterfaceAudience.Private
class StripedBlockReconstructor extends StripedReconstructor
    implements Runnable {
  private final RecoveryTable recoveryTable = new RecoveryTable();

  private OurECLogger ourECLogger = OurECLogger.getInstance();
  private StripedWriter stripedWriter;

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
    // [TODO] Clean.
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
      mergeData(toReconstructLen);
      // reconstructTargets();
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
    reconstructTargets();
  }


  ByteBuffer[] receivedByteBuffers = new ByteBuffer[getStripedReader().numberOfInputs()];
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
      ByteBuffer receivedByteBuffer = receivedByteBuffers[i];
      ByteBuffer newInput = inputs[i];
      if (newInput == null) {
        continue;
      }
      if (receivedByteBuffer != null) {
        ByteBuffer[] temps = new ByteBuffer[] {receivedByteBuffer, newInput};
        int totalCapacity = receivedByteBuffer.capacity() + newInput.capacity();
        receivedByteBuffers[i] = concat(temps, totalCapacity);
      } else {
        receivedByteBuffers[i] = clone(newInput);
      }
    }

  }

  private void mergeData(int toReconstructLen) throws IOException {
    ByteBuffer[] inputs = getStripedReader().getInputBuffers(toReconstructLen);
    updateByteBuffers(inputs);
  }

  private void reconstructTargets() throws IOException {
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
  }

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
}
