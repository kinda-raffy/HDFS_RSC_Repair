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

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.server.datanode.DataNodeFaultInjector;
import org.apache.hadoop.hdfs.server.datanode.metrics.DataNodeMetrics;
import org.apache.hadoop.io.erasurecode.rawcoder.RawErasureDecoder;
import org.apache.hadoop.io.erasurecode.rawcoder.InvalidDecodingException;
import org.apache.hadoop.util.OurECLogger;
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
      ourECLogger.write(this, getDatanode().getDatanodeUuid(), "run reconstructing 1");
      initDecoderIfNecessary();
      ourECLogger.write(this, getDatanode().getDatanodeUuid(), "run reconstructing 2");

      initDecodingValidatorIfNecessary();
      ourECLogger.write(this, getDatanode().getDatanodeUuid(), "run reconstructing 3");

      getStripedReader().init();
      ourECLogger.write(this, getDatanode().getDatanodeUuid(), "run reconstructing 4");

      stripedWriter.init();
      ourECLogger.write(this, getDatanode().getDatanodeUuid(), "run reconstructing 5");

      reconstruct();
      ourECLogger.write(this, getDatanode().getDatanodeUuid(), "run reconstructing 6");

      stripedWriter.endTargetBlocks();
      ourECLogger.write(this, getDatanode().getDatanodeUuid(), "run reconstructing 7");

      // Currently we don't check the acks for packets, this is similar as
      // block replication.
    } catch (Throwable e) {
      String errorStackTrace = ExceptionUtils.getStackTrace(e);
      ourECLogger.write(this, getDatanode().getDatanodeUuid(), "reconstructing exception: " + errorStackTrace);

      LOG.warn("Failed to reconstruct striped block: {}", getBlockGroup(), e);
      getDatanode().getMetrics().incrECFailedReconstructionTasks();
    } finally {
      ourECLogger.write(this, getDatanode().getDatanodeUuid(), "reconstructing final 1");

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
      ourECLogger.write(this, getDatanode().getDatanodeUuid(), "reconstructing final 2");

      getStripedReader().close();
      stripedWriter.close();
      cleanup();
    }
  }

  @Override
  void reconstruct() throws IOException {
    // [TODO] Clean.

    ourECLogger.write(this, getDatanode().getDatanodeUuid(), "start reconstructing");

    while (getPositionInBlock() < getMaxTargetLength()) {
      ourECLogger.write(this, getDatanode().getDatanodeUuid(), "while loop reconstructing 1-getPositionInBlock: " +
              getPositionInBlock() + "/" + getMaxTargetLength());
      DataNodeFaultInjector.get().stripedBlockReconstruction();
      long remaining = getMaxTargetLength() - getPositionInBlock();
      final int toReconstructLen =
          (int) Math.min(getStripedReader().getBufferSize(), remaining);

      ourECLogger.write(this, getDatanode().getDatanodeUuid(), "while loop reconstructing 1.5-bufferSize: " +
              getStripedReader().getBufferSize() + "/" + remaining);
      ourECLogger.write(this, getDatanode().getDatanodeUuid(), "while loop reconstructing 2-getPositionInBlock: " +
              getPositionInBlock() + "/" + getMaxTargetLength());

      long start = Time.monotonicNow();
      long bytesToRead = (long) toReconstructLen * getStripedReader().getMinRequiredSources();
      if (getDatanode().getEcReconstuctReadThrottler() != null) {
        getDatanode().getEcReconstuctReadThrottler().throttle(bytesToRead);
      }
      // step1: read from minimum source DNs required for reconstruction.
      // The returned success list is the source DNs we do real read from
      getStripedReader().readMinimumSources(toReconstructLen);
      long readEnd = Time.monotonicNow();
      ourECLogger.write(this, getDatanode().getDatanodeUuid(), "while loop reconstructing 3-toReconstructLen: " + toReconstructLen);

      // step2: decode to reconstruct targets
      reconstructTargets(toReconstructLen);
      long decodeEnd = Time.monotonicNow();
      ourECLogger.write(this, getDatanode().getDatanodeUuid(), "while loop reconstructing 4");

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
      // Need to reconstruct: 1048576 bytes
      // After the first loop: 1048064 bytes
      // After the second loop: 512 bytes

      // Only the succeed reconstructions are recorded.
      final DataNodeMetrics metrics = getDatanode().getMetrics();
      metrics.incrECReconstructionReadTime(readEnd - start);
      metrics.incrECReconstructionDecodingTime(decodeEnd - readEnd);
      metrics.incrECReconstructionWriteTime(writeEnd - decodeEnd);
      updatePositionInBlock(toReconstructLen);
      ourECLogger.write(this, getDatanode().getDatanodeUuid(), "while loop reconstructing 5-getPositionInBlock: " +
              getPositionInBlock() + "/" + getMaxTargetLength());
      clearBuffers();
    }
  }

  private void reconstructTargets(int toReconstructLen) throws IOException {
    ByteBuffer[] inputs = getStripedReader().getInputBuffers(toReconstructLen);

    int[] erasedIndices = stripedWriter.getRealTargetIndices();
    ByteBuffer[] outputs = stripedWriter.getRealTargetBuffers(toReconstructLen);

    if (isValidationEnabled()) {
      markBuffers(inputs);
      decode(inputs, erasedIndices, outputs);
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
      RawErasureDecoder decoder = getDecoder();
      ourECLogger.write(this, getDatanode().getDatanodeUuid(), "reconstructTargets - decoder: " + decoder);
      decode(inputs, erasedIndices, outputs);
    }
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
