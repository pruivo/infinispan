package org.infinispan.hadoopintegration.mapreduce.output;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.util.Progressable;
import org.infinispan.hadoopintegration.InfinispanCache;
import org.infinispan.hadoopintegration.configuration.InfinispanConfiguration;

import java.io.IOException;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 7.0
 */
public class InfinispanOutputFormat<K, V> implements OutputFormat<K, V>, Configurable {

    private static Log log = LogFactory.getLog(InfinispanOutputFormat.class);

    private InfinispanConfiguration configuration;

    @Override
    public RecordWriter<K, V> getRecordWriter(FileSystem fileSystem, JobConf entries, String s, Progressable progressable) throws IOException {
        InfinispanOutputConverter<K, V, Object, Object> converter;
        try {
            converter = configuration.getOutputConverter();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        InfinispanCache<Object, Object> infinispanCache = InfinispanCache.getOutputCache(configuration);
        log.info("Creating Record Writer with " + infinispanCache);
        return new InfinispanRecordWriter<K, V, Object, Object>(infinispanCache, converter);
    }

    @Override
    public void checkOutputSpecs(FileSystem fileSystem, JobConf entries) throws IOException {
        //nothing to validate for now.
    }

    @Override
    public void setConf(Configuration entries) {
        this.configuration = new InfinispanConfiguration(entries);
    }

    @Override
    public Configuration getConf() {
        return configuration.getConfiguration();
    }
}
