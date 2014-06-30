package org.infinispan.hadoopintegration.sample;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.infinispan.hadoopintegration.mapreduce.input.InfinispanInputConverter;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 7.0
 */
public class InputConverter implements InfinispanInputConverter<LongWritable, Text, Integer, String> {
    @Override
    public LongWritable createKey() {
        return new LongWritable();
    }

    @Override
    public Text createValue() {
        return new Text();
    }

    @Override
    public void setKey(LongWritable longWritable, Integer integer) {
        longWritable.set(integer);
    }

    @Override
    public void setValue(Text text, String s) {
        text.set(s);
    }
}
