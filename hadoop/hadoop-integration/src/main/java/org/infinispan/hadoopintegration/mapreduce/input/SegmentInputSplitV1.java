package org.infinispan.hadoopintegration.mapreduce.input;


import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 7.0
 */
public class SegmentInputSplitV1 implements SegmentInputSplit {
    private List<Integer> segmentsId;
    private String[] hostName;

    public SegmentInputSplitV1() {
    }

    public SegmentInputSplitV1(SegmentOwners owners, List<Integer> segmentsId) {
        this.segmentsId = new ArrayList<Integer>(segmentsId);
        this.hostName = new String[owners.size()];
        int index;
        index = 0;
        for (SocketAddress address : owners) {
            this.hostName[index++] = ((InetSocketAddress) address).getHostName();
        }
    }

    @Override
    public long getLength() throws IOException {
        return segmentsId.size();
    }

    @Override
    public String[] getLocations() throws IOException {
        return hostName;
    }

    @Override
    public List<Integer> getSegmentsId() {
        return segmentsId;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(segmentsId.size());
        for (int i : segmentsId) {
            dataOutput.writeInt(i);
        }
        dataOutput.writeInt(hostName.length);
        for (String s : hostName) {
            dataOutput.writeUTF(s);
        }
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        int size = dataInput.readInt();
        this.segmentsId = new ArrayList<Integer>(size);
        for (int i = 0; i < size; ++i) {
            segmentsId.add(dataInput.readInt());
        }
        size = dataInput.readInt();
        this.hostName = new String[size];
        for (int i = 0; i < size; ++i) {
            hostName[i] = dataInput.readUTF();
        }
    }

    @Override
    public String toString() {
        return "SegmentInputSplit{" +
                "segmentsId=" + segmentsId +
                ", hostName=" + Arrays.toString(hostName) +
                '}';
    }
}
