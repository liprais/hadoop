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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.server.namenode.snapshot.Snapshot;

/**
 * Default (backward-compatible) {@link INodeStore} that delegates to the
 * existing in-memory {@link INodeMap} and {@link INodeDirectory#children}
 * structures.
 *
 * <p>This implementation is used when no external store is configured.
 * It preserves the original NameNode memory behaviour exactly.
 */
@InterfaceAudience.Private
public class InMemoryINodeStore implements INodeStore {

  /**
   * The backing {@link INodeMap}.  Set by
   * {@link FSDirectory} after the map is constructed.
   */
  private INodeMap inodeMap;

  InMemoryINodeStore() {}

  /**
   * Bind this store to an already-constructed {@link INodeMap}.
   * Called from {@link FSDirectory} after the INodeMap is created.
   */
  void setINodeMap(INodeMap map) {
    this.inodeMap = map;
  }

  // -----------------------------------------------------------------------
  // INodeStore implementation  — delegates to the in-memory INodeMap
  // -----------------------------------------------------------------------

  @Override
  public void initialize() throws IOException {
    // Nothing to do for in-memory store.
  }

  @Override
  public boolean isInitialized() {
    return inodeMap != null;
  }

  @Override
  public void put(INode inode) throws IOException {
    if (inodeMap != null) {
      inodeMap.putInternal(inode);
    }
  }

  @Override
  public void remove(long inodeId) throws IOException {
    if (inodeMap != null) {
      INode inode = inodeMap.get(inodeId);
      if (inode != null) {
        inodeMap.removeInternal(inode);
      }
    }
  }

  @Override
  public INode get(long inodeId) throws IOException {
    return inodeMap == null ? null : inodeMap.get(inodeId);
  }

  /**
   * For the in-memory implementation, children are maintained directly in
   * {@link INodeDirectory#children} and are NOT retrieved from this store.
   * This method is provided for API completeness; callers that need directory
   * children should use {@link INodeDirectory#getChildrenList} instead.
   */
  @Override
  public List<INode> getChildren(long parentId) throws IOException {
    INode parent = get(parentId);
    if (parent == null || !parent.isDirectory()) {
      return Collections.emptyList();
    }
    return new ArrayList<>(
        parent.asDirectory().getChildrenList(Snapshot.CURRENT_STATE_ID));
  }

  @Override
  public INode getChild(long parentId, byte[] localName) throws IOException {
    INode parent = get(parentId);
    if (parent == null || !parent.isDirectory()) {
      return null;
    }
    return parent.asDirectory().getChild(localName, Snapshot.CURRENT_STATE_ID);
  }

  @Override
  public int size() throws IOException {
    return inodeMap == null ? 0 : inodeMap.size();
  }

  @Override
  public void clear() throws IOException {
    if (inodeMap != null) {
      inodeMap.clear();
    }
  }

  @Override
  public Iterator<INodeWithAdditionalFields> iterator() throws IOException {
    return inodeMap == null
        ? java.util.Collections.emptyIterator()
        : inodeMap.getMapIterator();
  }

  @Override
  public void close() throws IOException {
    // Nothing to close.
  }
}
