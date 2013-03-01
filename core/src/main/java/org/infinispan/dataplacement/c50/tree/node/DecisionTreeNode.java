package org.infinispan.dataplacement.c50.tree.node;

import org.infinispan.dataplacement.c50.keyfeature.Feature;
import org.infinispan.dataplacement.c50.keyfeature.FeatureValue;

import java.io.Serializable;
import java.util.Map;

/**
 * Interface of a Decision Tree node
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public interface DecisionTreeNode extends Serializable {

   /**
    * find and return the best node that respect the feature values
    *
    * @param keyFeatures the feature values
    * @return the best node
    */
   DecisionTreeNode find(Map<Feature, FeatureValue> keyFeatures);

   /**
    * returns the value of the node (i.e. the new owner)
    *
    * @return the value of the node (i.e. the new owner)
    */
   int getValue();

   /**
    * @return the deep of the sub-tree where this node is the root
    */
   int getDeep();
}
