package org.infinispan.hadoopintegration.mapreduce.input;

import org.apache.hadoop.mapred.InputSplit;

import java.util.List;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 7.0
 */
public interface SegmentInputSplit extends InputSplit {

    List<Integer> getSegmentsId();
}
