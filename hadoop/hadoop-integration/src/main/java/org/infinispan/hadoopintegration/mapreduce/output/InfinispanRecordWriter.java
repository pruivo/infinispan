package org.infinispan.hadoopintegration.mapreduce.output;

import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.infinispan.hadoopintegration.InfinispanCache;

import java.io.IOException;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 7.0
 */
public class InfinispanRecordWriter<K, V, K1, V1> implements RecordWriter<K, V> {

    private final InfinispanCache<K1, V1> infinispanCache;
    private final InfinispanOutputConverter<K, V, K1, V1> converter;

    public InfinispanRecordWriter(InfinispanCache<K1, V1> infinispanCache, InfinispanOutputConverter<K, V, K1, V1> converter) {
        this.infinispanCache = infinispanCache;
        this.converter = converter;
    }

    @Override
    public void write(K k, V v) throws IOException {
        infinispanCache.getRemoteCache().put(converter.convertKey(k),
                converter.convertValue(v));
    }

    @Override
    public void close(Reporter reporter) throws IOException {
        infinispanCache.stop();
    }
}
