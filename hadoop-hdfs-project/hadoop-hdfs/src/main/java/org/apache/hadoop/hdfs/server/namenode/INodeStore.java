/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.server.namenode;

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * Pluggable interface for the persistent storage of HDFS NameNode INode
 * metadata.
 *
 * <h2>Architecture</h2>
 * The NameNode maintains an in-memory representation of the entire filesystem
 * namespace (INode tree). For large clusters this can require tens of gigabytes
 * of heap. {@code INodeStore} decouples the <em>access API</em> (path
 * resolution, listing, permission checks — unchanged) from the
 * <em>storage layer</em>, allowing the namespace metadata to live in an
 * external store such as PostgreSQL.
 *
 * <h2>Implementations</h2>
 * <ul>
 *   <li>{@link InMemoryINodeStore} – default, wraps the existing in-memory
 *       {@link INodeMap} and {@link INodeDirectory#children} structures.</li>
 *   <li>{@link PostgreSQLINodeStore} – stores inode metadata in PostgreSQL,
 *       enabling the NameNode heap to remain nearly constant as the number of
 *       files grows.</li>
 * </ul>
 *
 * <h2>Configuration</h2>
 * Set {@code dfs.namenode.inode.store.class} to the fully-qualified class name
 * of the desired {@code INodeStore} implementation. When the property is
 * absent or empty, {@link InMemoryINodeStore} is used.
 *
 * <h2>Memory-saving strategy</h2>
 * With an external {@code INodeStore}:
 * <ol>
 *   <li>All INode metadata is written to the external store whenever an inode
 *       is created or modified.</li>
 *   <li>The in-memory children list of an {@link INodeDirectory} can be
 *       <em>evicted</em> and reloaded on demand via
 *       {@link #getChildren(long)}.</li>
 *   <li>The {@link INodeMap} can similarly evict INode objects and reload
 *       them via {@link #get(long)}.</li>
 * </ol>
 * This way, only the "hot" portion of the namespace needs to reside in heap.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public interface INodeStore extends Closeable {

  /**
   * Initialise the backing store.
   * For PostgreSQL this creates the required tables if they do not yet exist.
   *
   * @throws IOException if the store cannot be initialised
   */
  void initialize() throws IOException;

  /**
   * Persist (insert or update) an inode.
   *
   * <p>The inode's parent must already be set (via
   * {@link INode#setParent(INode)}) before this method is called, because
   * the parent INode ID is used as the {@code parent_id} column value.
   * The root inode has no parent and its parent ID is stored as {@code -1}.
   *
   * @param inode the INode to persist; must be an
   *              {@link INodeWithAdditionalFields}
   * @throws IOException if the write fails
   */
  void put(INode inode) throws IOException;

  /**
   * Remove an inode by its ID.
   * If the inode does not exist, the call is a no-op.
   *
   * @param inodeId the ID of the inode to remove
   * @throws IOException if the removal fails
   */
  void remove(long inodeId) throws IOException;

  /**
   * Retrieve an inode by its ID.
   *
   * @param inodeId the ID to look up
   * @return the INode, or {@code null} if not found
   * @throws IOException if the lookup fails
   */
  INode get(long inodeId) throws IOException;

  /**
   * Return all immediate children of a directory, sorted by name (ascending,
   * lexicographic byte order — same ordering as the in-memory children list).
   *
   * @param parentId the ID of the parent directory
   * @return mutable list of child inodes, never {@code null}
   * @throws IOException if the query fails
   */
  List<INode> getChildren(long parentId) throws IOException;

  /**
   * Return a specific child of a directory by its local name.
   *
   * @param parentId  the ID of the parent directory
   * @param localName the child's local name as raw bytes
   * @return the child INode, or {@code null} if not found
   * @throws IOException if the query fails
   */
  INode getChild(long parentId, byte[] localName) throws IOException;

  /**
   * @return the total number of inodes currently stored
   * @throws IOException if the count query fails
   */
  int size() throws IOException;

  /**
   * Remove all stored inodes.
   * Used during namespace reset (e.g. {@code bin/hdfs namenode -format}).
   *
   * @throws IOException if the operation fails
   */
  void clear() throws IOException;

  /**
   * Return an iterator over every stored inode.
   * Used by {@link INodeMap#getMapIterator()} for FSImage serialisation.
   *
   * @throws IOException if an error occurs building the result set
   */
  Iterator<INodeWithAdditionalFields> iterator() throws IOException;

  /**
   * @return {@code true} if {@link #initialize()} has been called successfully
   */
  boolean isInitialized();
}
