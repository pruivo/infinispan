package org.infinispan.hadoopintegration.sample;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.infinispan.hadoopintegration.mapreduce.output.InfinispanOutputConverter;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 7.0
 */
public class OutputConverter implements InfinispanOutputConverter<Text, IntWritable, String, Integer> {

    @Override
    public String convertKey(Text text) {
        return text.toString();
    }

    @Override
    public Integer convertValue(IntWritable intWritable) {
        return intWritable.get();
    }
}
