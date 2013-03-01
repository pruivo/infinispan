package org.infinispan.dataplacement.c50.tree;

import org.infinispan.dataplacement.c50.keyfeature.Feature;
import org.infinispan.dataplacement.c50.keyfeature.FeatureValue;
import org.infinispan.dataplacement.c50.tree.node.DecisionTreeNode;
import org.infinispan.dataplacement.c50.tree.node.Type0Node;
import org.infinispan.dataplacement.c50.tree.node.Type1Node;
import org.infinispan.dataplacement.c50.tree.node.Type2Node;
import org.infinispan.dataplacement.c50.tree.node.Type3Node;

import java.util.Map;

/**
 * Builds a queryable decision tree based on the parsed tree
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public class DecisionTreeBuilder {
   /**
    * returns a queryable decision tree that represents the parsed tree
    *
    * @param root       the root node
    * @param featureMap the feature map to translate the attributes names in Feature instances
    * @return the queryable decision tree
    */
   public static DecisionTree build(ParseTreeNode root, Map<String, Feature> featureMap) {
      DecisionTreeNode node = parseNode(root, featureMap);
      return new DecisionTree(node);
   }

   private static DecisionTreeNode parseNode(ParseTreeNode node, Map<String, Feature> featureMap) {
      switch (node.getType()) {
         case 0:
            return parseType0Node(node);
         case 1:
            return parseType1Node(node, featureMap);
         case 2:
            return parseType2Node(node, featureMap);
         case 3:
            return parseType3Node(node, featureMap);
         default:
            throw new IllegalArgumentException("Unkown parsed node type");
      }
   }

   private static DecisionTreeNode parseType0Node(ParseTreeNode node) {
      return new Type0Node(getValue(node));
   }

   private static DecisionTreeNode parseType1Node(ParseTreeNode node, Map<String, Feature> featureMap) {
      return new Type1Node(getValue(node), getFeature(node, featureMap), getForks(node, featureMap));
   }

   private static DecisionTreeNode parseType2Node(ParseTreeNode node, Map<String, Feature> featureMap) {
      Feature feature = getFeature(node, featureMap);
      return new Type2Node(getValue(node), feature, getForks(node, featureMap), getCut(node, feature));
   }

   private static DecisionTreeNode parseType3Node(ParseTreeNode node, Map<String, Feature> featureMap) {
      return new Type3Node(getValue(node), getFeature(node, featureMap), getForks(node, featureMap), node.getElts());
   }

   private static int getValue(ParseTreeNode node) {
      return Integer.parseInt(node.getClazz());
   }

   private static Feature getFeature(ParseTreeNode node, Map<String, Feature> featureMap) {
      Feature feature = featureMap.get(node.getAttribute());

      if (feature == null) {
         throw new IllegalStateException("Unknown attribute " + node.getAttribute());
      }
      return feature;
   }

   /*
   Decision tree nodes. Each type node has it own find rules and each node has it own attribute (except the leaf node)
    */

   private static DecisionTreeNode[] getForks(ParseTreeNode node, Map<String, Feature> featureMap) {
      DecisionTreeNode[] forks = new DecisionTreeNode[node.getNumberOfForks()];
      ParseTreeNode[] parseForks = node.getForks();

      for (int i = 0; i < forks.length; ++i) {
         forks[i] = parseNode(parseForks[i], featureMap);
      }

      return forks;
   }

   private static FeatureValue getCut(ParseTreeNode node, Feature feature) {
      String cut = node.getCut();
      return feature.featureValueFromParser(cut);
   }
}
