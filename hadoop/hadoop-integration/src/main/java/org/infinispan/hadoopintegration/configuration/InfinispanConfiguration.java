package org.infinispan.hadoopintegration.configuration;

import org.apache.hadoop.conf.Configuration;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.impl.ConfigurationProperties;
import org.infinispan.hadoopintegration.mapreduce.input.InfinispanInputConverter;
import org.infinispan.hadoopintegration.mapreduce.output.InfinispanOutputConverter;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 7.0
 */
public class InfinispanConfiguration {

    private static final String INPUT_SPLIT_REMOTE_CACHE_HOST = "mapreduce.ispn.inputsplit.remote.cache.host";
    private static final String INPUT_SPLIT_REMOTE_CACHE_PORT = "mapreduce.ispn.inputsplit.remote.cache.port";

    private static final String INPUT_REMOTE_CACHE_HOST = "mapreduce.ispn.input.remote.cache.host";
    private static final String INPUT_REMOTE_CACHE_PORT = "mapreduce.ispn.input.remote.cache.port";
    private static final String INPUT_REMOTE_CACHE_NAME = "mapreduce.ispn.input.cache.name";
    private static final String INPUT_CONVERTER = "mapreduce.ispn.input.converter";

    private static final String OUTPUT_REMOTE_CACHE_HOST = "mapreduce.ispn.output.remote.cache.host";
    private static final String OUTPUT_REMOTE_CACHE_PORT = "mapreduce.ispn.output.remote.cache.port";
    private static final String OUTPUT_REMOTE_CACHE_NAME = "mapreduce.ispn.output.cache.name";
    private static final String OUTPUT_CONVERTER = "mapreduce.ispn.output.converter";

    private final Configuration configuration;

    public InfinispanConfiguration(Configuration configuration) {
        this.configuration = configuration;
    }

    public Configuration getConfiguration() {
        return configuration;
    }

    public String getInputSplitRemoteCacheHost() {
        return configuration.get(INPUT_SPLIT_REMOTE_CACHE_HOST, "localhost");
    }

    public int getInputSplitRemoteCachePort() {
        return configuration.getInt(INPUT_SPLIT_REMOTE_CACHE_PORT, ConfigurationProperties.DEFAULT_HOTROD_PORT);
    }


    public String getInputRemoteCacheHost() {
        return configuration.get(INPUT_REMOTE_CACHE_HOST, "localhost");
    }

    public int getInputRemoteCachePort() {
        return configuration.getInt(INPUT_REMOTE_CACHE_PORT, ConfigurationProperties.DEFAULT_HOTROD_PORT);
    }

    public String getInputCacheName() {
        return configuration.get(INPUT_REMOTE_CACHE_NAME, RemoteCacheManager.DEFAULT_CACHE_NAME);
    }

    public String getOutputRemoteCacheHost() {
        return configuration.get(OUTPUT_REMOTE_CACHE_HOST, "localhost");
    }

    public int getOutputRemoteCachePort() {
        return configuration.getInt(OUTPUT_REMOTE_CACHE_PORT, ConfigurationProperties.DEFAULT_HOTROD_PORT);
    }

    public String getOutputCacheName() {
        return configuration.get(OUTPUT_REMOTE_CACHE_NAME, RemoteCacheManager.DEFAULT_CACHE_NAME);
    }

    public <K, V, K1, V1> InfinispanInputConverter<K, V, K1, V1> getInputConverter() throws ClassNotFoundException,
            IllegalAccessException, InstantiationException {
        String className = configuration.get(INPUT_CONVERTER);
        if (className == null) {
            throw new IllegalArgumentException(INPUT_CONVERTER + " is expected!");
        }
        Class<InfinispanInputConverter> clazz = (Class<InfinispanInputConverter>) Class.forName(className);
        return clazz.newInstance();
    }

    public <K, V, K1, V1> InfinispanOutputConverter<K, V, K1, V1> getOutputConverter() throws ClassNotFoundException,
            IllegalAccessException, InstantiationException {
        String className = configuration.get(OUTPUT_CONVERTER);
        if (className == null) {
            throw new IllegalArgumentException(OUTPUT_CONVERTER + " is expected!");
        }
        Class<InfinispanOutputConverter> clazz = (Class<InfinispanOutputConverter>) Class.forName(className);
        return clazz.newInstance();
    }


}
