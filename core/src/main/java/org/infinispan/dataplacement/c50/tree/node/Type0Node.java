package org.infinispan.dataplacement.c50.tree.node;

import org.infinispan.dataplacement.c50.keyfeature.Feature;
import org.infinispan.dataplacement.c50.keyfeature.FeatureValue;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.Map;

/**
 * Type 0 decision tree node: the leaf node
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public class Type0Node implements DecisionTreeNode {

   private static final Log log = LogFactory.getLog(Type0Node.class);
   private final int value;

   public Type0Node(int value) {
      this.value = value;
   }

   @Override
   public DecisionTreeNode find(Map<Feature, FeatureValue> keyFeatures) {
      if (log.isTraceEnabled()) {
         log.tracef("Try to find key [%s]. returning %s", keyFeatures, value);
      }
      return null; //it is a leaf node
   }

   @Override
   public int getValue() {
      return value;
   }

   @Override
   public int getDeep() {
      return 0;
   }
}
