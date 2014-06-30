package org.infinispan.hadoopintegration.mapreduce.input;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.RecordReader;
import org.infinispan.client.hotrod.impl.consistenthash.ConsistentHash;
import org.infinispan.hadoopintegration.InfinispanCache;

import java.io.IOException;
import java.util.*;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 7.0
 */
public class SegmentRecordReader<K, V, K1, V1> implements RecordReader<K, V> {

    private static Log log = LogFactory.getLog(SegmentRecordReader.class);
    private static final boolean trace = log.isTraceEnabled();

    private List<K1> keys;
    private final List<Integer> segmentList;
    private List<Map.Entry<K1, V1>> entryList;
    private final InfinispanCache<K1, V1> infinispanCache;
    private final InfinispanInputConverter<K, V, K1, V1> converter;
    private int keyIndex;
    private int entryIndex;
    private int segmentIndex;
    private Map.Entry<K1, V1> current;


    public SegmentRecordReader(InputSplit inputSplit, InfinispanCache<K1, V1> infinispanCache, InfinispanInputConverter<K, V, K1, V1> converter) {
        this.infinispanCache = infinispanCache;
        this.converter = converter;
        this.keyIndex = -1;
        this.segmentList = new ArrayList<Integer>(((SegmentInputSplit) inputSplit).getSegmentsId());
        this.entryList = Collections.emptyList();
    }

    @Override
    public boolean next(K kInfinispanObject, V vInfinispanObject) throws IOException {
        if (!nextKeyValue()) {
            return false;
        }

        if (trace) {
            log.trace("Setting " + current);
        }
        converter.setKey(kInfinispanObject, current.getKey());
        converter.setValue(vInfinispanObject, current.getValue());
        return true;
    }

    @Override
    public K createKey() {
        return converter.createKey();
    }

    @Override
    public V createValue() {
        return converter.createValue();
    }

    @Override
    public long getPos() throws IOException {
        return keyIndex;
    }

    @Override
    public void close() throws IOException {
        infinispanCache.stop();
    }

    private void initialize(InputSplit inputSplit) {
        List<Integer> segmentIds = ((SegmentInputSplit) inputSplit).getSegmentsId();
        if (segmentIds.equals(InfinispanInputFormat.ALL_SEGMENTS)) {
            if (trace) {
                log.trace("Initializing Record Read with all keys");
            }
            keys = new ArrayList<K1>(infinispanCache.getRemoteCache().keySet());
        } else {
            if (trace) {
                log.trace("Initializing Record Read with segments " + segmentIds);
            }
            ConsistentHash consistentHash = infinispanCache.getRemoteCache().getConsistentHash();
            Set<Integer> segmentsIds = new HashSet<Integer>(segmentIds);
            keys = new LinkedList<K1>();

            for (K1 key : infinispanCache.getRemoteCache().keySet()) {
                if (trace) {
                    log.trace("Checking " + key + ". segmentId(key)=" + segmentOf(key, consistentHash));
                }
                if (segmentsIds.contains(segmentOf(key, consistentHash))) {
                    keys.add(key);
                }
            }
            if (trace) {
                log.trace("Record Read initialized! Keys are " + keys);
            }
        }
    }

    private static int segmentOf(Object key, ConsistentHash consistentHash) {
        return consistentHash.getSegment(key);
    }

    public boolean nextKeyValue() {
        current = null;
        while (!hasNextEntry()) {
            if (!hasNextSegment()) {
                if (trace) {
                    log.trace("No more keys to read!");
                }
                return false;
            }
            int segmentId = nextSegment();
            if (trace) {
                log.trace("Fetching entries for segment " + segmentId);
            }
            setEntryList(infinispanCache.getRemoteCache().getSegment(segmentId).entrySet());
        }
        current = nextEntry();
        return true;
    }

    @Override
    public float getProgress() throws IOException {
        if (segmentList.size() == 0) {
            return 1f;
        }
        return segmentIndex / segmentList.size();
    }

    private boolean hasNextEntry() {
        return entryIndex < entryList.size();
    }

    private Map.Entry<K1, V1> nextEntry() {
        if (!hasNextEntry()) {
            throw new NoSuchElementException();
        }
        return entryList.get(entryIndex++);
    }

    private void setEntryList(Collection<Map.Entry<K1, V1>> entryCollection) {
        if (trace) {
            log.trace("New entry list: " + entryCollection);
        }
        this.entryList = new ArrayList<Map.Entry<K1, V1>>(entryCollection);
        this.entryIndex = 0;
    }

    private boolean hasNextSegment() {
        return segmentIndex < segmentList.size();
    }

    private int nextSegment() {
        if (!hasNextSegment()) {
            throw new NoSuchElementException();
        }
        return segmentList.get(segmentIndex++);
    }

}
