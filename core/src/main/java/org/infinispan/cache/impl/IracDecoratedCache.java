package org.infinispan.cache.impl;

import java.util.function.Function;

import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commons.util.EnumUtil;
import org.infinispan.context.Flag;
import org.infinispan.functional.impl.MetaParamsInternalMetadata;
import org.infinispan.metadata.impl.IracMetaParam;
import org.infinispan.metadata.impl.IracMetadata;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 10.1
 */
public class IracDecoratedCache<K, V> extends DecoratedCache<K, V> {
   private static final long IRAC_FLAG_BITSET = EnumUtil.bitSetOf(Flag.IGNORE_RETURN_VALUES, Flag.SKIP_XSITE_BACKUP, Flag.IRAC_UPDATE);

   private final SetIracMetadataForPut transformPut;
   private final SetIracMetadataForRemove transformRemove;

   public IracDecoratedCache(CacheImpl<K, V> delegate, IracMetadata iracMetadata) {
      super(delegate, IRAC_FLAG_BITSET);
      this.transformPut = new SetIracMetadataForPut(iracMetadata);
      this.transformRemove = new SetIracMetadataForRemove(iracMetadata);
   }

   @Override
   protected Function<PutKeyValueCommand, PutKeyValueCommand> getPutKeyValueTransform() {
      return transformPut;
   }

   @Override
   protected Function<RemoveCommand, RemoveCommand> getRemoveKeyTransform() {
      return transformRemove;
   }

   private static class SetIracMetadataForPut implements Function<PutKeyValueCommand, PutKeyValueCommand> {

      private final IracMetadata iracMetadata;

      private SetIracMetadataForPut(IracMetadata iracMetadata) {
         this.iracMetadata = iracMetadata;
      }

      @Override
      public PutKeyValueCommand apply(PutKeyValueCommand command) {
         MetaParamsInternalMetadata.Builder builder = new MetaParamsInternalMetadata.Builder();
         builder.add(new IracMetaParam(iracMetadata));
         command.setInternalMetadata(builder.build());
         return command;
      }

      @Override
      public String toString() {
         return "SetIracMetadataForPut{" +
                "iracMetadata=" + iracMetadata +
                '}';
      }
   }

   private static class SetIracMetadataForRemove implements Function<RemoveCommand, RemoveCommand> {

      private final IracMetadata iracMetadata;

      private SetIracMetadataForRemove(IracMetadata iracMetadata) {
         this.iracMetadata = iracMetadata;
      }

      @Override
      public RemoveCommand apply(RemoveCommand command) {
         MetaParamsInternalMetadata.Builder builder = new MetaParamsInternalMetadata.Builder();
         builder.add(new IracMetaParam(iracMetadata));
         command.setInternalMetadata(builder.build());
         return command;
      }

      @Override
      public String toString() {
         return "SetIracMetadataForRemove{" +
                "iracMetadata=" + iracMetadata +
                '}';
      }
   }


}
