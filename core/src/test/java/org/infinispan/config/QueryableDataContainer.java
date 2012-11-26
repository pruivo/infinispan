/*
 * JBoss, Home of Professional Open Source
 * Copyright 2011 Red Hat Inc. and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.infinispan.config;

import org.infinispan.container.versioning.EntryVersion;
import org.infinispan.metadata.Metadata;
import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.InternalCacheEntry;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.Set;

import static java.util.Collections.synchronizedCollection;

public class QueryableDataContainer implements DataContainer {
	
	private static DataContainer delegate;
	
	public static void setDelegate(DataContainer delegate) {
	   QueryableDataContainer.delegate = delegate;
   }
	
	private final Collection<String> loggedOperations;
	
	public void setFoo(String foo) {
		loggedOperations.add("setFoo(" + foo + ")");
	}

	public QueryableDataContainer() {
	   this.loggedOperations = synchronizedCollection(new ArrayList<String>());
   }
	
	@Override
	public Iterator<InternalCacheEntry> iterator() {
		loggedOperations.add("iterator()");
		return delegate.iterator();
	}

	@Override
	public InternalCacheEntry get(Object k, Metadata metadata) {
		loggedOperations.add("get(" + k + "," + metadata + ")" );
		return delegate.get(k, null);
	}

	@Override
	public InternalCacheEntry peek(Object k, Metadata metadata) {
		loggedOperations.add("peek(" + k + "," + metadata + ")" );
		return delegate.peek(k, null);
	}

	@Override
	public void put(Object k, Object v, Metadata metadata) {
		loggedOperations.add("put(" + k + ", " + v + ", " + metadata + ")");
		delegate.put(k, v, metadata);
	}

	@Override
	public boolean containsKey(Object k, Metadata metadata) {
		loggedOperations.add("containsKey(" + k + ", " + metadata + ")" );
		return delegate.containsKey(k, null);
	}

	@Override
	public InternalCacheEntry remove(Object k, Metadata metadata) {
		loggedOperations.add("remove(" + k + ", " + metadata + ")" );
		return delegate.remove(k, null);
	}

	@Override
	public int size(Metadata metadata) {
		loggedOperations.add("size(" + metadata + ")" );
		return delegate.size(null);
	}

	@Override
	public void clear() {
		loggedOperations.add("clear()" );
		delegate.clear();
	}

	@Override
	public Set<Object> keySet(Metadata metadata) {
		loggedOperations.add("keySet(" + metadata + ")" );
		return delegate.keySet(null);
	}

	@Override
	public Collection<Object> values(Metadata metadata) {
		loggedOperations.add("values(" + metadata+ ")" );
		return delegate.values(null);
	}

	@Override
	public Set<InternalCacheEntry> entrySet(Metadata metadata) {
		loggedOperations.add("entrySet( " + metadata + ")" );
		return delegate.entrySet(null);
	}

	@Override
	public void purgeExpired() {
		loggedOperations.add("purgeExpired()" );
		delegate.purgeExpired();
	}

   @Override
   public void clear(Metadata metadata) {
      loggedOperations.add("clear(" + metadata + ")");
      delegate.clear(metadata);
   }
	
   @Override
   public boolean dumpTo(String filePath) {
      return delegate.dumpTo(filePath);
   }

   @Override
   public void purgeOldValues(EntryVersion minimumVersion) {
      delegate.purgeOldValues(minimumVersion);
   }

   public Collection<String> getLoggedOperations() {
	   return loggedOperations;
   }
}
