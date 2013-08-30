package org.infinispan.dataplacement;

import org.infinispan.dataplacement.ch.LCRDClusterUtil;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Arrays;

/**
 * @author Pedro Ruivo
 * @since 5.2
 */
@Test(groups = "unit", testName = "dataplacement.DRDUtilTest")
public class DRDUtilTest {

   private static final Log log = LogFactory.getLog(DRDUtilTest.class);

   public void testStartIndexAndSize() {
      final float[] weight = new float[] {0.9f, 0.1f};
      assertSize(new int[] {0,0}, new int[] {1,1}, weight, 1, 1);
      assertSize(new int[] {0,1}, new int[] {1,1}, weight, 2, 1);
      assertSize(new int[] {0,2}, new int[] {2,1}, weight, 3, 1);
      assertSize(new int[] {0,3}, new int[] {3,1}, weight, 4, 1);
      assertSize(new int[] {0,4}, new int[] {4,1}, weight, 5, 1);
      assertSize(new int[] {0,5}, new int[] {5,1}, weight, 6, 1);
      assertSize(new int[] {0,6}, new int[] {6,1}, weight, 7, 1);
      assertSize(new int[] {0,7}, new int[] {7,1}, weight, 8, 1);
      assertSize(new int[] {0,8}, new int[] {8,1}, weight, 9, 1);
      assertSize(new int[] {0,9}, new int[] {9,1}, weight, 10, 1);
   }

   public void testStartIndexAndSize2() {
      final float[] weight = new float[] {0.1f, 0.9f};
      assertSize(new int[] {0,0}, new int[] {1,1}, weight, 1, 1);
      assertSize(new int[] {0,1}, new int[] {1,1}, weight, 2, 1);
      assertSize(new int[] {0,1}, new int[] {1,2}, weight, 3, 1);
      assertSize(new int[] {0,1}, new int[] {1,3}, weight, 4, 1);
      assertSize(new int[] {0,1}, new int[] {1,4}, weight, 5, 1);
      assertSize(new int[] {0,1}, new int[] {1,5}, weight, 6, 1);
      assertSize(new int[] {0,1}, new int[] {1,6}, weight, 7, 1);
      assertSize(new int[] {0,1}, new int[] {1,7}, weight, 8, 1);
      assertSize(new int[] {0,1}, new int[] {1,8}, weight, 9, 1);
      assertSize(new int[] {0,1}, new int[] {1,9}, weight, 10, 1);
   }

   private void assertSize(int[] startIndex, int[] clusterSize, float[] weight, int numNodes, int numOwners) {
      log.debugf("Start index=%s, cluster size=%s, weight=%s, num nodes=%s, num owners=%s",
                 Arrays.toString(startIndex), Arrays.toString(clusterSize), Arrays.toString(weight),
                 numNodes, numOwners);
      Assert.assertEquals(startIndex.length, clusterSize.length);
      Assert.assertEquals(startIndex.length, weight.length);
      Assert.assertTrue(numNodes >= 1);
      Assert.assertTrue(numOwners >= 1);

      for (int i = 0; i < startIndex.length; ++i) {
         Assert.assertEquals(LCRDClusterUtil.startIndex(weight, i, numNodes), startIndex[i],
                             "Wrong start index for " + i);
         Assert.assertEquals(LCRDClusterUtil.clusterSize(weight, i, numNodes, numOwners), clusterSize[i],
                             "Wrong cluster size for " + i);
      }
   }

}
