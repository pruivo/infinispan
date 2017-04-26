package org.infinispan.xsite.async;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.List;

import org.infinispan.AdvancedCache;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.marshall.MarshallUtil;
import org.infinispan.context.Flag;
import org.infinispan.util.ByteString;
import org.infinispan.xsite.BackupReceiver;
import org.infinispan.xsite.XSiteReplicateCommand;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 8.0
 */
public class AsyncUpdateCommand extends XSiteReplicateCommand {

   private List<AsyncUpdate> updates;

   public AsyncUpdateCommand(ByteString cacheName, int maxSize) {
      super(cacheName);
      updates = new ArrayList<>(maxSize);
   }

   @Override
   public Object performInLocalSite(BackupReceiver receiver) throws Throwable {
      AdvancedCache<Object, Object> cache = receiver.getCache().getAdvancedCache()
            .withFlags(Flag.SKIP_XSITE_BACKUP, Flag.IGNORE_RETURN_VALUES);
      List<Object> failedKeys = new ArrayList<>(updates.size());
      for (AsyncUpdate update : updates) {
         try {
            update.perform(cache);
         } catch (CacheException e) {
            failedKeys.add(update.getKey());
         }
      }
      return failedKeys.isEmpty() ? null : failedKeys;
   }

   public int addUpdate(AsyncUpdate update) {
      updates.add(update);
      return updates.size();
   }

   @Override
   public byte getCommandId() {
      return 0;  // TODO: Customise this generated block
   }

   @Override
   public boolean isReturnValueExpected() {
      return true;
   }

   @Override
   public void writeTo(ObjectOutput output) throws IOException {
      MarshallUtil.marshallCollection(updates, output, AsyncUpdate::writeTo);
   }

   @Override
   public void readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      updates = MarshallUtil.unmarshallCollection(input, ArrayList::new, AsyncUpdate::readFrom);
   }
}
