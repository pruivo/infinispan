package org.infinispan.metadata.impl;

import java.util.Objects;

import org.infinispan.functional.MetaParam;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 10.1
 */
public class IracMetaParam implements MetaParam<IracMetadata> {

   private final IracMetadata meta;

   public IracMetaParam(IracMetadata meta) {
      this.meta = meta;
   }

   @Override
   public IracMetadata get() {
      return meta;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) {
         return true;
      }
      if (o == null || getClass() != o.getClass()) {
         return false;
      }

      IracMetaParam that = (IracMetaParam) o;
      return Objects.equals(meta, that.meta);
   }

   @Override
   public int hashCode() {
      return meta != null ? meta.hashCode() : 0;
   }

   @Override
   public String toString() {
      return "IracMetaParam=" + meta;
   }
}
