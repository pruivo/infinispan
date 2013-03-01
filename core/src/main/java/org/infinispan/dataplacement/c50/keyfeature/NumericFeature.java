package org.infinispan.dataplacement.c50.keyfeature;

import java.text.NumberFormat;
import java.text.ParseException;

import static java.lang.Double.compare;

/**
 * Implements a Feature that has as values a number
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public class NumericFeature implements Feature {

   private static final String[] CLASSES = new String[]{"continuous"};
   private final String name;

   public NumericFeature(String name) {
      if (name == null) {
         throw new IllegalArgumentException("Null is not a valid Feature name");
      }
      this.name = name.replaceAll("\\s", "");
   }

   @Override
   public String getName() {
      return name;
   }

   @Override
   public String[] getMachineLearnerClasses() {
      return CLASSES;
   }

   @Override
   public FeatureValue createFeatureValue(Object value) {
      if (value instanceof Number) {
         return new NumericValue((Number) value);
      }
      throw new IllegalArgumentException("Expected a number type value");
   }

   @Override
   public FeatureValue featureValueFromParser(String value) {
      try {
         Number number = NumberFormat.getNumberInstance().parse(value);
         return new NumericValue(number);
      } catch (ParseException e) {
         throw new IllegalStateException("Error parsing value from decision tree");
      }
   }

   @Override
   public String toString() {
      return "NumericFeature{" +
            "name='" + name + '\'' +
            '}';
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      NumericFeature that = (NumericFeature) o;

      return name != null ? name.equals(that.name) : that.name == null;

   }

   @Override
   public int hashCode() {
      return name != null ? name.hashCode() : 0;
   }

   public static class NumericValue implements FeatureValue {

      private final Number value;

      private NumericValue(Number value) {
         this.value = value;
      }

      @Override
      public boolean isLessOrEqualsThan(FeatureValue other) {
         return other instanceof NumericValue &&
               compare(value.doubleValue(), ((NumericValue) other).value.doubleValue()) <= 0;
      }

      @Override
      public boolean isGreaterThan(FeatureValue other) {
         return other instanceof NumericValue &&
               compare(value.doubleValue(), ((NumericValue) other).value.doubleValue()) > 0;
      }

      @Override
      public boolean isEquals(FeatureValue other) {
         return other instanceof NumericValue &&
               compare(value.doubleValue(), ((NumericValue) other).value.doubleValue()) == 0;
      }

      @Override
      public String getValueAsString() {
         return value.toString();
      }

      @Override
      public String toString() {
         return "NumericValue{" +
               "value=" + value +
               '}';
      }
   }
}
