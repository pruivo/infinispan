package org.infinispan.dataplacement;

import junit.framework.Assert;
import org.infinispan.dataplacement.c50.lookup.BloomFilter;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;

/**
 * Bloom Filter Test
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
@Test(groups = "functional", testName = "dataplacement.BloomFilterTest")
public class BloomFilterTest {

   private static final Log log = LogFactory.getLog(BloomFilterTest.class);
   private static final int START = 10;
   private static final int END = 100000;
   private static final double PROB = 0.001;
   private static final int NUMBER_OF_KEYS = 1000;

   public void testBloomFilter() {

      for (int iteration = START; iteration <= END; iteration *= 10) {
         LinkedList<Object> linkedList = new LinkedList<Object>();
         ArrayList<Object> arrayList = new ArrayList<Object>();
         HashSet<Object> hashSet = new HashSet<Object>();

         for (int key = 0; key < iteration; ++key) {
            linkedList.add(getKey(key));
         }
         log.infof("[%s] Linked List serialized size (bytes) = %s", iteration, serializedSize(linkedList));

         BloomFilter bloomFilter = new BloomFilter(linkedList, PROB);
         linkedList.clear();

         log.infof("=== Iterator with %s objects ===", iteration);
         log.infof("Bloom filter size = %s", bloomFilter.size());

         long begin = System.currentTimeMillis();
         for (int key = 0; key < iteration; ++key) {
            Assert.assertTrue("False Negative should not happen!", bloomFilter.contains(getKey(key)));
         }
         long end = System.currentTimeMillis();

         log.infof("Query duration:\n\ttotal=%s ms\n\tper-key=%s ms", end - begin, (end - begin) / iteration);

         log.infof("[%s] Bloom Filter serialized size (bytes) = %s", iteration, serializedSize(bloomFilter));

         for (int key = 0; key < iteration; ++key) {
            arrayList.add(getKey(key));
         }
         log.infof("[%s] Array List serialized size (bytes) = %s", iteration, serializedSize(arrayList));
         arrayList.clear();

         for (int key = 0; key < iteration; ++key) {
            hashSet.add(getKey(key));
         }
         log.infof("[%s] Hash Set serialized size (bytes) = %s", iteration, serializedSize(hashSet));
         hashSet.clear();
      }

   }

   public void testSerializable() throws IOException, ClassNotFoundException {
      List<Object> keys = new ArrayList<Object>(NUMBER_OF_KEYS);
      for (int i = 0; i < NUMBER_OF_KEYS; ++i) {
         keys.add(getKey(i));
      }

      BloomFilter bloomFilter = new BloomFilter(keys, PROB);

      ByteArrayOutputStream arrayOutputStream = new ByteArrayOutputStream();
      ObjectOutputStream oos = new ObjectOutputStream(arrayOutputStream);

      oos.writeObject(bloomFilter);
      oos.flush();
      oos.close();

      byte[] bytes = arrayOutputStream.toByteArray();

      log.infof("Bloom filter size with %s keys is %s bytes", NUMBER_OF_KEYS, bytes.length);

      ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(bytes));

      BloomFilter readBloomFilter = (BloomFilter) ois.readObject();
      ois.close();

      for (int i = 0; i < NUMBER_OF_KEYS; ++i) {
         Assert.assertTrue("[ORIGINAL] False negative should never happen. It happened in original",
                           bloomFilter.contains(getKey(i)));
         Assert.assertTrue("[COPIED] False negative should never happen. It happened in original",
                           readBloomFilter.contains(getKey(i)));
      }
   }

   private String getKey(int index) {
      return "KEY_" + index + "_" + (index * 2) + "_" + (index * 3);
   }

   private int serializedSize(Object object) {
      ByteArrayOutputStream arrayOutputStream = new ByteArrayOutputStream();

      try {
         ObjectOutputStream oos = new ObjectOutputStream(arrayOutputStream);
         oos.writeObject(object);
         oos.flush();
         oos.close();
      } catch (IOException e) {
         return -1;
      }

      return arrayOutputStream.toByteArray().length;
   }

}
