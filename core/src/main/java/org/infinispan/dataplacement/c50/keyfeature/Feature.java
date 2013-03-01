package org.infinispan.dataplacement.c50.keyfeature;

import java.io.Serializable;

/**
 * Represents an interface for a key feature
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public interface Feature extends Serializable {

   /**
    * returns the feature name
    *
    * @return the feature name
    */
   String getName();

   /**
    * returns the classes for the machine learner, corresponding to this feature (attribute)
    *
    * @return the classes for the machine learner
    */
   String[] getMachineLearnerClasses();

   /**
    * creates a feature value of the type of this attribute
    *
    * @param value the value
    * @return the feature value corresponding to {@code value}
    */
   FeatureValue createFeatureValue(Object value);

   /**
    * creates a feature value from the value parsed from the decision tree
    *
    * @param value the value
    * @return the feature value corresponding to {@code value}
    */
   FeatureValue featureValueFromParser(String value);
}
