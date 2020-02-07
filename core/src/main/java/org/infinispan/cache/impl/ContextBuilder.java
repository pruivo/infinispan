package org.infinispan.cache.impl;

import org.infinispan.context.InvocationContext;

/**
 * //TODO document this!
 *
 * @author Pedro Ruivo
 * @since 9.0
 */
public interface ContextBuilder {

   InvocationContext create(int keyCount);
}
