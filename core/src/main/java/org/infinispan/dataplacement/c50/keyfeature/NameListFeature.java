package org.infinispan.dataplacement.c50.keyfeature;

import java.util.Arrays;

/**
 * Implements a Feature that has as values a list of names
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public class NameListFeature implements Feature {

   private final String[] classes;
   private final String name;

   public NameListFeature(String name, String... classes) {
      if (name == null) {
         throw new IllegalArgumentException("Null not allowed as Feature name");
      }

      if (classes == null || classes.length <= 1) {
         throw new IllegalArgumentException("Expected non-null and more than one classes");
      }

      this.name = name.replaceAll("\\s", "");
      this.classes = new String[classes.length];

      for (int i = 0; i < classes.length; ++i) {
         this.classes[i] = classes[i].replaceAll("\\s,[.]:[|]", "");
      }
   }

   @Override
   public String getName() {
      return name;
   }

   @Override
   public String[] getMachineLearnerClasses() {
      return classes;
   }

   @Override
   public FeatureValue createFeatureValue(Object value) {
      if (value instanceof String) {
         return new StringValue((String) value);
      }
      throw new IllegalArgumentException("Expected a String type value");
   }

   @Override
   public FeatureValue featureValueFromParser(String value) {
      return new StringValue(value);
   }

   @Override
   public String toString() {
      return "NameListFeature{" +
            "name='" + name + '\'' +
            ", classes=" + (classes == null ? null : Arrays.asList(classes)) +
            '}';
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      NameListFeature that = (NameListFeature) o;

      return Arrays.equals(classes, that.classes) && (name != null ? name.equals(that.name) : that.name == null);

   }

   @Override
   public int hashCode() {
      int result = classes != null ? Arrays.hashCode(classes) : 0;
      result = 31 * result + (name != null ? name.hashCode() : 0);
      return result;
   }

   public static class StringValue implements FeatureValue {

      private final String value;

      private StringValue(String value) {
         this.value = value;
      }

      @Override
      public boolean isLessOrEqualsThan(FeatureValue other) {
         return false;
      }

      @Override
      public boolean isGreaterThan(FeatureValue other) {
         return false;
      }

      @Override
      public boolean isEquals(FeatureValue other) {
         return other instanceof StringValue && value.equals(((StringValue) other).value);
      }

      @Override
      public String getValueAsString() {
         return value;
      }

      @Override
      public String toString() {
         return "StringValue{" +
               "value='" + value + '\'' +
               '}';
      }
   }
}
