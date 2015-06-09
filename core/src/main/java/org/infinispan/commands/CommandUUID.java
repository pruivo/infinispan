package org.infinispan.commands;

import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.marshall.core.Ids;
import org.infinispan.remoting.transport.Address;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 8.0
 */
public class CommandUUID {

   private static final AtomicLong nextId = new AtomicLong(0);

   private final Address address;
   private final long id;

   private CommandUUID(Address address, long id) {
      this.address = address;
      this.id = id;
   }

   public static CommandUUID generateUUID(Address address) {
      return new CommandUUID(address, nextId.getAndIncrement());
   }

   public static CommandUUID generateUUIDFrom(CommandUUID commandUUID) {
      return new CommandUUID(commandUUID.address, nextId.getAndIncrement());
   }


   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      CommandUUID that = (CommandUUID) o;

      if (id != that.id) return false;
      return !(address != null ? !address.equals(that.address) : that.address != null);

   }

   @Override
   public int hashCode() {
      int result = address != null ? address.hashCode() : 0;
      result = 31 * result + (int) (id ^ (id >>> 32));
      return result;
   }

   @Override
   public String toString() {
      return "CommandUUID{" +
            "address=" + address +
            ", id=" + id +
            '}';
   }

   public static class Externalizer extends AbstractExternalizer<CommandUUID> {

      @Override
      public Set<Class<? extends CommandUUID>> getTypeClasses() {
         return Collections.singleton(CommandUUID.class);
      }

      @Override
      public void writeObject(ObjectOutput output, CommandUUID object) throws IOException {
         output.writeObject(object.address);
         output.writeLong(object.id);
      }

      @Override
      public CommandUUID readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         Address address = (Address) input.readObject();
         long id = input.readLong();
         return new CommandUUID(address, id);
      }

      @Override
      public Integer getId() {
         return Ids.COMMAND_UUID;
      }
   }
}
