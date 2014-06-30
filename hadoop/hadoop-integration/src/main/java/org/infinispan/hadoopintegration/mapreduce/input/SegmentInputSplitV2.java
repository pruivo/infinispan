package org.infinispan.hadoopintegration.mapreduce.input;


import org.apache.hadoop.mapred.InputSplit;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 7.0
 */
public class SegmentInputSplitV2 implements SegmentInputSplit {
    private List<Integer> segmentsId;
    private String[] locations;

    public SegmentInputSplitV2() {
    }

    public SegmentInputSplitV2(String hostName, List<Integer> segmentsId) {
        this.segmentsId = new ArrayList<Integer>(segmentsId);
        this.locations = new String[]{hostName};
    }

    private SegmentInputSplitV2(String[] locations, List<Integer> segmentsId) {

    }

    @Override
    public long getLength() throws IOException {
        return segmentsId.size();
    }

    @Override
    public String[] getLocations() throws IOException {
        return locations;
    }

    @Override
    public List<Integer> getSegmentsId() {
        return segmentsId;
    }

    public void splitSegments(List<InputSplit> list) {
        int size = segmentsId.size();
        if (size > 2) {
            int middleIndex = size / 2;
            list.add(new SegmentInputSplitV2(locations, new ArrayList<Integer>(segmentsId.subList(0, middleIndex))));
            list.add(new SegmentInputSplitV2(locations, new ArrayList<Integer>(segmentsId.subList(middleIndex, segmentsId.size()))));
        } else {
            list.add(this);
        }
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(segmentsId.size());
        for (int i : segmentsId) {
            dataOutput.writeInt(i);
        }
        dataOutput.writeUTF(locations[0]);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        int size = dataInput.readInt();
        this.segmentsId = new ArrayList<Integer>(size);
        for (int i = 0; i < size; ++i) {
            segmentsId.add(dataInput.readInt());
        }
        this.locations = new String[]{dataInput.readUTF()};
    }

    @Override
    public String toString() {
        return "SegmentInputSplit{" +
                "segmentsId=" + segmentsId +
                ", locations=" + Arrays.toString(locations) +
                '}';
    }
}
