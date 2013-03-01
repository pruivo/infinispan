package org.infinispan.dataplacement.c50.tree.node;

import org.infinispan.dataplacement.c50.keyfeature.Feature;
import org.infinispan.dataplacement.c50.keyfeature.FeatureValue;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.Arrays;
import java.util.Map;

/**
 * Type 1 decision tree node: discrete attributes
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public class Type1Node implements DecisionTreeNode {

   private static final Log log = LogFactory.getLog(Type1Node.class);
   private final int value;
   private final Feature feature;
   private final DecisionTreeNode[] forks;
   private final FeatureValue[] attributeValues;

   public Type1Node(int value, Feature feature, DecisionTreeNode[] forks) {
      this.value = value;
      this.feature = feature;
      if (forks == null || forks.length == 0) {
         throw new IllegalArgumentException("Expected a non-null with at least one fork");
      }

      String[] possibleValues = feature.getMachineLearnerClasses();

      if (forks.length != possibleValues.length + 1) {
         throw new IllegalArgumentException("Number of forks different from the number of possible values");
      }

      this.forks = forks;
      attributeValues = new FeatureValue[forks.length - 1];

      for (int i = 0; i < attributeValues.length; ++i) {
         attributeValues[i] = feature.featureValueFromParser(possibleValues[i]);
      }
   }

   @Override
   public DecisionTreeNode find(Map<Feature, FeatureValue> keyFeatures) {
      if (log.isTraceEnabled()) {
         log.tracef("Try to find key [%s] with feature %s", keyFeatures, feature);
      }

      FeatureValue keyValue = keyFeatures.get(feature);
      if (keyValue == null) { //N/A
         if (log.isTraceEnabled()) {
            log.tracef("Feature Not Available...");
         }
         return forks[0];
      }

      if (log.isTraceEnabled()) {
         log.tracef("Comparing key value [%s] with possible values %s", keyValue, Arrays.asList(attributeValues));
      }

      for (int i = 0; i < attributeValues.length; ++i) {
         if (attributeValues[i].isEquals(keyValue)) {
            if (log.isTraceEnabled()) {
               log.tracef("Next decision tree found. The value in %s matched", i);
            }
            return forks[i + 1];
         }
      }

      throw new IllegalStateException("Expected one value match");
   }

   @Override
   public int getValue() {
      return value;
   }

   @Override
   public int getDeep() {
      int maxDeep = 0;
      for (DecisionTreeNode decisionTreeNode : forks) {
         maxDeep = Math.max(maxDeep, decisionTreeNode.getDeep());
      }
      return maxDeep + 1;
   }
}
