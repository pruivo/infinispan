package org.infinispan.dataplacement;

import org.infinispan.dataplacement.c50.keyfeature.Feature;
import org.infinispan.dataplacement.c50.keyfeature.FeatureValue;
import org.infinispan.dataplacement.c50.keyfeature.KeyFeatureManager;
import org.infinispan.dataplacement.c50.keyfeature.NumericFeature;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Dummy Key Feature manager
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public class DummyKeyFeatureManager implements KeyFeatureManager {
   private final Feature[] features = new Feature[]{
         new NumericFeature("B"),
         new NumericFeature("C")
   };

   public static Object getKey(int c) {
      return String.format("KEY_%s", c);
   }

   public static Object getKey(int b, int c) {
      return String.format("KEY_%s_%s", b, c);
   }

   @Override
   public Feature[] getAllKeyFeatures() {
      return features;
   }

   @Override
   public Map<Feature, FeatureValue> getFeatures(Object key) {

      if (!(key instanceof String)) {
         return Collections.emptyMap();
      }

      String[] split = ((String) key).split("_");
      Map<Feature, FeatureValue> features = new HashMap<Feature, FeatureValue>();

      if (split.length == 3) {
         int b = Integer.parseInt(split[1]);
         int c = Integer.parseInt(split[2]);

         features.put(this.features[0], this.features[0].createFeatureValue(b));
         features.put(this.features[1], this.features[1].createFeatureValue(c));
      } else if (split.length == 2) {
         int c = Integer.parseInt(split[1]);

         features.put(this.features[1], this.features[1].createFeatureValue(c));
      }

      return features;
   }
}
