package org.infinispan.counter.impl.raft.operation;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.infinispan.commons.io.ByteBuffer;
import org.infinispan.commons.io.EmptyByteBuffer;
import org.infinispan.counter.api.CounterConfiguration;
import org.infinispan.counter.api.CounterType;
import org.infinispan.counter.api.Storage;
import org.infinispan.counter.impl.raft.RaftCounter;
import org.infinispan.util.ByteString;

/**
 * TODO!
 */
public class CreateCounterOperation implements RaftCounterOperation<Void> {

   private final ByteString name;
   private final CounterConfiguration configuration;

   public CreateCounterOperation(ByteString name, CounterConfiguration configuration) {
      this.name = name;
      this.configuration = configuration;
   }

   @Override
   public byte getOperationCode() {
      return OperationCode.CREATE_IF_ABSENT;
   }

   @Override
   public ByteString getCounterName() {
      return name;
   }

   @Override
   public ByteBuffer execute(RaftCounter counter) {
      return EmptyByteBuffer.INSTANCE;
   }

   @Override
   public int serializedSize() {
      // counter name + 1 (flags) + 8 (initial value) + 8 (lower bound) + 8 (upper bound)
      return name.serializedSize() + 25;
   }

   @Override
   public void writeTo(DataOutput output) throws IOException {
      ByteString.writeObject(output, name);
      writeConfiguration(configuration, output);
   }

   @Override
   public Void readResult(ByteBuffer buffer) {
      return null;
   }

   public CounterConfiguration getConfiguration() {
      return configuration;
   }

   public static CreateCounterOperation readFrom(DataInput input) throws IOException {
      return new CreateCounterOperation(ByteString.readObject(input), readConfiguration(input));
   }

   private static void writeConfiguration(CounterConfiguration configuration, DataOutput output) throws IOException {
      output.writeByte(encodeTypeAndStorage(configuration.type(), configuration.storage()));
      output.writeLong(configuration.initialValue());
      if (configuration.type() == CounterType.BOUNDED_STRONG) {
         output.writeLong(configuration.lowerBound());
         output.writeLong(configuration.upperBound());
      }
   }

   private static CounterConfiguration readConfiguration(DataInput input) throws IOException {
      byte flags = input.readByte();
      CounterType type = decodeType(flags);
      CounterConfiguration.Builder builder = CounterConfiguration.builder(type);
      builder.storage(decodeStorage(flags));
      builder.initialValue(input.readLong());
      if (type == CounterType.BOUNDED_STRONG) {
         builder.lowerBound(input.readLong());
         builder.upperBound(input.readLong());
      }
      return builder.build();
   }

   private static byte encodeTypeAndStorage(CounterType type, Storage storage) {
      assert type != CounterType.WEAK;
      return (byte) ((type == CounterType.BOUNDED_STRONG ? 0x01 : 0x00) | (storage == Storage.VOLATILE ? 0x10 : 0x00));
   }

   private static CounterType decodeType(byte flags) {
      return (flags & 0x0f) == 0x01 ? CounterType.BOUNDED_STRONG : CounterType.UNBOUNDED_STRONG;
   }

   private static Storage decodeStorage(byte flags) {
      return (flags & 0xf0) == 0x10 ? Storage.VOLATILE : Storage.PERSISTENT;
   }
}
