/*
 * JBoss, Home of Professional Open Source
 * Copyright 2009 Red Hat Inc. and/or its affiliates and other
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
package org.infinispan.context;

import org.infinispan.container.entries.gmu.InternalGMUCacheEntry;
import org.infinispan.container.versioning.EntryVersion;
import org.infinispan.container.versioning.VersionGenerator;

import org.infinispan.remoting.transport.Address;

import java.util.Map;
import java.util.Set;

/**
 * A context that contains information pertaining to a given invocation.  These contexts typically have the lifespan of
 * a single invocation.
 *
 * @author Manik Surtani (<a href="mailto:manik@jboss.org">manik@jboss.org</a>)
 * @author Mircea.Markus@jboss.com
 * @author Pedro Ruivo
 * @author Sebastiano Peluso
 * @since 4.0
 */
public interface InvocationContext extends EntryLookup, Cloneable {

   /**
    * Returns true if the call was originated locally, false if it is the result of a remote rpc.
    */
   boolean isOriginLocal();
   
   /**
    * Get the origin of the command, or null if the command originated locally
    * @return
    */
   Address getOrigin();

   /**
    * Returns true if this call is performed in the context of an transaction, false otherwise.
    */
   boolean isInTxScope();

   /**
    * Returns the in behalf of which locks will be aquired.
    */
   Object getLockOwner();

   /**
    * Indicates whether the call requires a {@link java.util.concurrent.Future}
    * as return type.
    *
    * @return true if the call requires a {@link java.util.concurrent.Future}
    *              as return type, false otherwise
    */
   boolean isUseFutureReturnType();

   /**
    * Sets whether the call requires a {@link java.util.concurrent.Future}
    * as return type.
    *
    * @param useFutureReturnType boolean indicating whether a {@link java.util.concurrent.Future}
    *                            will be needed.
    */
   void setUseFutureReturnType(boolean useFutureReturnType);

   /**
    * Clones the invocation context.
    *
    * @return A cloned instance of this invocation context instance
    */
   InvocationContext clone();

   /**
    * Returns the set of keys that are locked for writing.
    */
   Set<Object> getLockedKeys();

   void clearLockedKeys();

   /**
    * Returns the class loader associated with this invocation
    *
    * @return a class loader instance or null if no class loader was
    *         specifically associated
    */
   ClassLoader getClassLoader();

   /**
    * Sets the class loader associated for this invocation
    *
    * @param classLoader
    */
   void setClassLoader(ClassLoader classLoader);

   /**
    * Tracks the given key as locked by this invocation context.
    */
   void addLockedKey(Object key);

   /**
    * Returns true if the lock being tested is already held in the current scope, false otherwise.
    *
    * @param key lock to test
    */
   boolean hasLockedKey(Object key);

   /**
    * Tries to replace the value of the wrapped entry associated with the given key in the context, if one exists.
    *
    * @return true if the context already contained a wrapped entry for which this value was changed, false otherwise.
    */
   boolean replaceValue(Object key, Object value);

   /**
    * add the key to a map between key and {@link InternalGMUCacheEntry}. This entry has all the information needed to calculate
    * the new transaction version. This is a temporary map and should be clear before (or after) each command
    *
    * @param entry   the entry read
    */
   void addKeyReadInCommand(Object key, InternalGMUCacheEntry entry);

   /**
    * clears the map between key and internal gmu entry
    */
   void clearKeyReadInCommand();

   /**
    * @return  all the key read since last clear
    */
   Map<Object, InternalGMUCacheEntry> getKeysReadInCommand();

   /**
    * @param versionGenerator the version generator
    * @return  the maximum {@link EntryVersion} to read     
    */
   EntryVersion calculateVersionToRead(VersionGenerator versionGenerator);

   /**
    * sets the version to read
    *
    * @param entryVersion  the version to read
    */
   void setVersionToRead(EntryVersion entryVersion);

   /**    
    * @return true if it has already read from this node
    */
   boolean hasAlreadyReadOnThisNode();

   /**    
    * @param value   true or false if it has already read from this node
    */
   void setAlreadyReadOnThisNode(boolean value);

   /**
    * sets the replication protocol to be used by the command
    *
    * @param protocolId the protocol ID
    */
   void setProtocolId(String protocolId);

   /**
    * returns the protocol ID to be used by the command
    *
    * @return  the protocol ID to be used by the command
    */
   String getProtocolId();
}
