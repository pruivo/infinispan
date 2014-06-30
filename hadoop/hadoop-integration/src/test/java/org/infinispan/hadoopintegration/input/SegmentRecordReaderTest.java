package org.infinispan.hadoopintegration.input;

import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.hadoopintegration.InfinispanCache;
import org.infinispan.hadoopintegration.mapreduce.input.InfinispanInputConverter;
import org.infinispan.hadoopintegration.mapreduce.input.SegmentInputSplit;
import org.infinispan.hadoopintegration.mapreduce.input.SegmentInputSplitV2;
import org.infinispan.hadoopintegration.mapreduce.input.SegmentRecordReader;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.util.*;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 7.0
 */
public class SegmentRecordReaderTest {

    private static final InfinispanInputConverter<Reference<Integer>, Reference<Integer>, Integer, Integer> INSTANCE = new TestInputConverter();

    @Test
    public void test() throws IOException {
        Logger.getRootLogger().setLevel(Level.ALL);
        Logger.getRootLogger().addAppender(new ConsoleAppender());
        SegmentInputSplit split = new SegmentInputSplitV2("test", Arrays.asList(0, 1, 2, 3, 4));
        final Map<Integer, SegmentMap<Integer, Integer>> testMap = new HashMap<Integer, SegmentMap<Integer, Integer>>();
        testMap.put(0, new SegmentMap<Integer, Integer>(Arrays.asList(1, 2, 3, 4), Arrays.asList(1, 2, 3, 4)));
        testMap.put(1, new SegmentMap<Integer, Integer>(Arrays.asList(11, 12, 13, 14), Arrays.asList(11, 12, 13, 14)));
        testMap.put(4, new SegmentMap<Integer, Integer>(Arrays.asList(41, 42, 43, 44), Arrays.asList(41, 42, 43, 44)));
        InfinispanCache<Integer, Integer> infinispanCache = createInfinispanCacheMock(testMap);

        SegmentRecordReader<Reference<Integer>, Reference<Integer>, Integer, Integer> reader =
                new SegmentRecordReader<Reference<Integer>, Reference<Integer>, Integer, Integer>(split, infinispanCache, INSTANCE);

        final Reference<Integer> key = INSTANCE.createKey();
        final Reference<Integer> value = INSTANCE.createValue();

        for (int i : Arrays.asList(0, 1, 4)) {
            for (int j = 1; j < 5; ++j) {
                Integer expected = (i * 10) + j;
                Assert.assertTrue("Next <" + i + "," + j + ">", reader.next(key, value));
                Assert.assertEquals("Wrong key for <" + i + "," + j + ">", expected, key.getValue());
                Assert.assertEquals("Wrong value for <" + i + "," + j + ">", expected, value.getValue());
            }
        }

        Assert.assertFalse(reader.next(key, value));

    }

    private static <K, V> InfinispanCache<K, V> createInfinispanCacheMock(final Map<Integer, SegmentMap<K, V>> maps) {
        //noinspection unchecked
        InfinispanCache<K, V> mockCache = Mockito.mock(InfinispanCache.class);
        //noinspection unchecked
        RemoteCache<K, V> mockRemoteCache = Mockito.mock(RemoteCache.class);
        Mockito.when(mockCache.getRemoteCache()).thenReturn(mockRemoteCache);
        Mockito.when(mockRemoteCache.getSegment(Mockito.anyInt())).thenAnswer(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                Integer segmentId = (Integer) invocationOnMock.getArguments()[0];
                SegmentMap<K, V> map = maps.get(segmentId);
                return map == null ? Collections.<K, V>emptyMap() : map.map;
            }
        });
        return mockCache;
    }

    private static class Reference<K> {
        K value;

        public K getValue() {
            return value;
        }

        public void setValue(K value) {
            this.value = value;
        }
    }

    private static class TestInputConverter implements InfinispanInputConverter<Reference<Integer>, Reference<Integer>, Integer, Integer> {

        @Override
        public Reference<Integer> createKey() {
            return new Reference<Integer>();
        }

        @Override
        public Reference<Integer> createValue() {
            return new Reference<Integer>();
        }

        @Override
        public void setKey(Reference<Integer> key, Integer key2) {
            key.setValue(key2);
        }

        @Override
        public void setValue(Reference<Integer> value, Integer value2) {
            value.setValue(value2);
        }
    }

    private static class SegmentMap<K, V> {
        private final Map<K, V> map;

        private SegmentMap(List<K> keys, List<V> values) {
            map = new LinkedHashMap<K, V>();
            for (int i = 0; i < keys.size(); ++i) {
                map.put(keys.get(i), values.get(i));
            }
        }

    }
}
