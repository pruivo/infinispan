package org.infinispan.hadoopintegration.mapreduce.input;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.*;
import org.infinispan.client.hotrod.impl.consistenthash.ConsistentHash;
import org.infinispan.hadoopintegration.InfinispanCache;
import org.infinispan.hadoopintegration.configuration.InfinispanConfiguration;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.*;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 7.0
 */
public class InfinispanInputFormat<K, V> implements InputFormat<K, V>, Configurable {

    private static Log log = LogFactory.getLog(InfinispanInputFormat.class);

    private InfinispanConfiguration configuration;

    public static final List<Integer> ALL_SEGMENTS = Collections.singletonList(-1);

    @Override
    public InputSplit[] getSplits(JobConf jobContext, int chunks) throws IOException {
        InfinispanCache<K, V> infinispanCache = InfinispanCache.getInputCacheForInputSplit(configuration);
        try {
            return calculateSplitsV2(infinispanCache, configuration, chunks);
        } finally {
            infinispanCache.stop();
        }
    }

    @Override
    public RecordReader<K, V> getRecordReader(InputSplit inputSplit, JobConf entries, Reporter reporter) throws IOException {
        InfinispanInputConverter<K, V, Object, Object> converter;
        try {
            converter = configuration.getInputConverter();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        InfinispanCache<Object, Object> infinispanCache = InfinispanCache.getInputCache(configuration);
        log.info("Creating record reader for input " + inputSplit + " and using " + infinispanCache);
        return new SegmentRecordReader<K, V, Object, Object>(inputSplit, infinispanCache, converter);
    }


    @Override
    public void setConf(Configuration entries) {
        this.configuration = new InfinispanConfiguration(entries);
    }

    @Override
    public Configuration getConf() {
        return configuration.getConfiguration();
    }

    private static InputSplit createSingleServerInputSplit(InfinispanConfiguration configuration) {
        SocketAddress[] addresses = new SocketAddress[1];
        addresses[0] = InetSocketAddress.createUnresolved(configuration.getInputRemoteCacheHost(),
                configuration.getInputRemoteCachePort());
        return new SegmentInputSplitV1(new SegmentOwners(addresses), ALL_SEGMENTS);
    }

    /**
     * creates the splits based on the primary owner.
     */
    private static InputSplit[] calculateSplitsV2(InfinispanCache<?, ?> infinispanCache,
                                                  InfinispanConfiguration configuration,
                                                  int chunks) {
        ConsistentHash consistentHash = infinispanCache.getRemoteCache().getConsistentHash();
        if (consistentHash == null) {
            return new InputSplit[]{new SegmentInputSplitV2(configuration.getInputRemoteCacheHost(), ALL_SEGMENTS)};
        }
        Map<String, List<Integer>> ownersToSegmentsMap = new HashMap<String, List<Integer>>();
        List<InputSplit> inputSplitList = new LinkedList<InputSplit>();
        int segmentId = 0;
        SocketAddress[][] segmentOwners = consistentHash.getSegmentOwners();
        log.info("Splitting " + segmentOwners.length + " segments!");
        for (SocketAddress[] owners : segmentOwners) {
            String hostName = ((InetSocketAddress) owners[0]).getHostString();
            //inputSplitList.add(new SegmentInputSplit(segmentOwners, Collections.singletonList(segmentId++)));
            List<Integer> segments = ownersToSegmentsMap.get(hostName);
            if (segments == null) {
                segments = new LinkedList<Integer>();
                ownersToSegmentsMap.put(hostName, segments);
            }
            segments.add(segmentId++);
        }

        for (Map.Entry<String, List<Integer>> entry : ownersToSegmentsMap.entrySet()) {
            inputSplitList.add(new SegmentInputSplitV2(entry.getKey(), entry.getValue()));
        }

        int previousSize;

        while ((previousSize = inputSplitList.size()) < chunks) {
            for (int i = 0; i < previousSize; ++i) {
                SegmentInputSplitV2 splitV2 = (SegmentInputSplitV2) inputSplitList.remove(0);
                splitV2.splitSegments(inputSplitList);
            }
            if (previousSize == inputSplitList.size()) {
                break;
            }
        }

        log.info("Created input list (" + chunks + "): " + inputSplitList);

        return inputSplitList.toArray(new InputSplit[inputSplitList.size()]);
    }

    private static InputSplit[] calculateSplitsV1(InfinispanCache<?, ?> infinispanCache,
                                                  InfinispanConfiguration configuration,
                                                  int chunks) {
        ConsistentHash consistentHash = infinispanCache.getRemoteCache().getConsistentHash();
        if (consistentHash == null) {
            return new InputSplit[]{createSingleServerInputSplit(configuration)};
        }
        Map<SegmentOwners, List<Integer>> ownersToSegmentsMap = new HashMap<SegmentOwners, List<Integer>>();
        List<InputSplit> inputSplitList = new LinkedList<InputSplit>();
        int segmentId = 0;
        for (SocketAddress[] owners : consistentHash.getSegmentOwners()) {
            SegmentOwners segmentOwners = new SegmentOwners(owners);
            //inputSplitList.add(new SegmentInputSplit(segmentOwners, Collections.singletonList(segmentId++)));
            List<Integer> segments = ownersToSegmentsMap.get(segmentOwners);
            if (segments == null) {
                segments = new LinkedList<Integer>();
                ownersToSegmentsMap.put(segmentOwners, segments);
            }
            segments.add(segmentId++);
        }

        for (Map.Entry<SegmentOwners, List<Integer>> entry : ownersToSegmentsMap.entrySet()) {
            inputSplitList.add(new SegmentInputSplitV1(entry.getKey(), entry.getValue()));
        }

        log.info("Created input list: " + inputSplitList);

        return inputSplitList.toArray(new InputSplit[inputSplitList.size()]);
    }


}
