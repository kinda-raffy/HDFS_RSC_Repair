package org.apache.hadoop.hdfs.protocol.datatransfer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.hadoop.hdfs.AppendTestUtil;
import org.junit.Test;
import org.apache.hadoop.thirdparty.com.google.common.primitives.Ints;

import static org.junit.Assert.*;

public class TestPacketTraceReceiver {

    private static final long OFFSET_IN_BLOCK = 12345L;
    private static final int SEQNO = 54321;

    private byte[] prepareFakeTracePacket(byte[] data, byte[] sums) throws IOException {

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);

        int packetLen = data.length + sums.length + Ints.BYTES;
        PacketHeader header = new PacketHeader(
                packetLen, OFFSET_IN_BLOCK, SEQNO, false, data.length, false);
        header.write(dos);

        dos.write(sums);
        //calculate trace of the data part
        //   for(byte b : data) {
        // 	System.out.println("Current byte is: "+b);

        //}
        dos.write(data);
        //flush to the underlying output stream
        dos.flush();
        //return bytearray of the packet content
        return baos.toByteArray();
    }

    private static byte[] remainingAsArray(ByteBuffer buf) {
        byte[] b = new byte[buf.remaining()];
        buf.get(b); // transfers bytes from buf to the destination byte array b
        return b;
    }

    @Test
    public void testReceiveAndMirror() throws IOException {
        PacketReceiver pr = new PacketReceiver(false);
        doTestReceiveAndMirror(pr, 400, 30);
        pr.close();
    }

    private void doTestReceiveAndMirror(PacketReceiver pr, int dataLen, int checksumsLen)
            throws IOException {
        final byte[] DATA = AppendTestUtil.initBuffer(dataLen);
        final byte[] CHECKSUMS = AppendTestUtil.initBuffer(checksumsLen);

        //trace to be computed and sent from the DN
        //we will generate a fake packet to imitate the trace
        byte[] packet =  prepareFakeTracePacket(DATA, CHECKSUMS);

        //Input stream for receiving the trace packet
        ByteArrayInputStream in = new ByteArrayInputStream(packet);
        System.out.println("Read fake packet into ByteArrayInputStream..");

        //encapsulate the read input stream into a packet
        pr.receiveNextPacket(in);

        ByteBuffer parsedData = pr.getDataSlice();

        //check that two object arrays are equal or not.
        //If they are not, throw an AssertionError
        assertArrayEquals(DATA, remainingAsArray(parsedData));

        ByteBuffer parsedChecksums = pr.getChecksumSlice();
        assertArrayEquals(CHECKSUMS, remainingAsArray(parsedChecksums));

        PacketHeader header = pr.getHeader();
        assertEquals(SEQNO, header.getSeqno());
        assertEquals(OFFSET_IN_BLOCK, header.getOffsetInBlock());
        assertEquals(dataLen + checksumsLen + Ints.BYTES, header.getPacketLen());
    }
}
