package org.infinispan.container;

import org.infinispan.context.InvocationContext;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public interface CommitContextEntries {

   void commitContextEntries(InvocationContext context, boolean skipOwnershipCheck, boolean isPutForStateTransfer);

}
