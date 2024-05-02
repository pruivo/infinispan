package org.infinispan.remoting.transport.jgroups;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.infinispan.commands.TracedCommand;
import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.commons.marshall.MarshallUtil;
import org.infinispan.marshall.core.Ids;
import org.infinispan.telemetry.InfinispanRemoteSpanContext;

public class TraceCommandWrapper {

   private static final MarshallUtil.ElementWriter<String> STRING_ELEMENT_WRITER = (output, element) -> MarshallUtil.marshallString(element, output);
   private static final MarshallUtil.ElementReader<String> STRING_ELEMENT_READER = MarshallUtil::unmarshallString;
   public static final AdvancedExternalizer<TraceCommandWrapper> EXTERNALIZER = new Externalizer();

   private final TracedCommand command;
   private final InfinispanRemoteSpanContext context;

   public TraceCommandWrapper(TracedCommand command, InfinispanRemoteSpanContext context) {
      this.command = command;
      this.context = context;
   }

   public TracedCommand getCommand() {
      return command;
   }

   private static final class Externalizer implements AdvancedExternalizer<TraceCommandWrapper> {

      @Override
      public Set<Class<? extends TraceCommandWrapper>> getTypeClasses() {
         return Set.of(TraceCommandWrapper.class);
      }

      @Override
      public Integer getId() {
         return Ids.TRACE_COMMAND_WRAPPER;
      }

      @Override
      public void writeObject(ObjectOutput output, TraceCommandWrapper object) throws IOException {
         Map<String, String> remotePropagation = null;
         if (object.context != null) {
            remotePropagation = object.context.context();
         }
         if (remotePropagation == null && object.command.getTraceCommandData() != null && object.command.getTraceCommandData().context() != null) {
            remotePropagation = object.command.getTraceCommandData().context().context();
         }
         MarshallUtil.marshallMap(remotePropagation, STRING_ELEMENT_WRITER, STRING_ELEMENT_WRITER, output);
         output.writeObject(object.command);
      }

      @Override
      public TraceCommandWrapper readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         Map<String, String> context = MarshallUtil.unmarshallMap(input, STRING_ELEMENT_READER, STRING_ELEMENT_READER, HashMap::new);
         TracedCommand cmd = (TracedCommand) input.readObject();
         if (context != null) {
            cmd.setTraceCommandData(new TracedCommand.TraceCommandData(null, new InfinispanRemoteSpanContext(Map.copyOf(context))));
         }
         return new TraceCommandWrapper(cmd, null);
      }
   }
}
