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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.namenode.snapshot.Snapshot;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Unit tests for the {@link INodeStore} abstraction and its default
 * {@link InMemoryINodeStore} implementation.
 *
 * <p>These tests do not require a live database; they exercise the store
 * contract through the in-memory implementation backed by a real
 * {@link INodeMap}. A separate integration test (not yet added) would cover
 * the {@link PostgreSQLINodeStore} against a live PostgreSQL instance.
 *
 * <h2>What is tested</h2>
 * <ul>
 *   <li>Factory creates the correct store type based on configuration.</li>
 *   <li>Factory defaults to {@link InMemoryINodeStore} when no class is set.</li>
 *   <li>Factory throws when a bad class name is specified.</li>
 *   <li>{@link INodeStore#put(INode)} / {@link INodeStore#get(long)} round-trip.</li>
 *   <li>{@link INodeStore#remove(long)} removes inodes.</li>
 *   <li>{@link INodeStore#clear()} empties the store.</li>
 *   <li>{@link INodeStore#size()} reflects the current count.</li>
 *   <li>{@link INodeDirectory#evictChildren()} + lazy reload cycle
 *       (with {@link InMemoryINodeStore}).</li>
 *   <li>Config key {@link DFSConfigKeys#DFS_NAMENODE_INODE_STORE_CLASS_KEY}
 *       is read correctly by the factory.</li>
 * </ul>
 */
public class TestINodeStore {

  private INodeMap inodeMap;
  private InMemoryINodeStore store;

  private static final PermissionStatus PERMS = PermissionStatus.createImmutable(
      "hdfs", "supergroup", FsPermission.createImmutable((short) 0755));

  @BeforeEach
  public void setUp() throws Exception {
    INodeDirectory root = new INodeDirectory(
        INodeId.ROOT_INODE_ID, INodeDirectory.ROOT_NAME, PERMS, 0L);
    inodeMap = INodeMap.newInstance(root);
    store = new InMemoryINodeStore();
    store.setINodeMap(inodeMap);
  }

  @AfterEach
  public void tearDown() throws Exception {
    if (store != null) {
      store.close();
    }
  }

  // -----------------------------------------------------------------------
  // Factory tests
  // -----------------------------------------------------------------------

  /**
   * When no class is configured the factory returns an InMemoryINodeStore.
   */
  @Test
  @Timeout(value = 10)
  public void testFactoryDefaultsToInMemory() throws Exception {
    Configuration conf = new Configuration();
    INodeStore created = INodeStoreFactory.create(conf);
    assertInstanceOf(InMemoryINodeStore.class, created,
        "Default store should be InMemoryINodeStore");
    created.close();
  }

  /**
   * Explicitly setting the class to InMemoryINodeStore should work.
   */
  @Test
  @Timeout(value = 10)
  public void testFactoryExplicitInMemory() throws Exception {
    Configuration conf = new Configuration();
    conf.set(DFSConfigKeys.DFS_NAMENODE_INODE_STORE_CLASS_KEY,
        InMemoryINodeStore.class.getName());
    INodeStore created = INodeStoreFactory.create(conf);
    assertInstanceOf(InMemoryINodeStore.class, created);
    created.close();
  }

  /**
   * A bad class name should throw IOException.
   */
  @Test
  @Timeout(value = 10)
  public void testFactoryBadClassThrows() {
    Configuration conf = new Configuration();
    conf.set(DFSConfigKeys.DFS_NAMENODE_INODE_STORE_CLASS_KEY,
        "com.example.NonExistentINodeStore");
    assertThrows(IOException.class, () -> INodeStoreFactory.create(conf));
  }

  /**
   * A class that doesn't implement INodeStore should throw IOException.
   */
  @Test
  @Timeout(value = 10)
  public void testFactoryWrongInterfaceThrows() {
    Configuration conf = new Configuration();
    conf.set(DFSConfigKeys.DFS_NAMENODE_INODE_STORE_CLASS_KEY,
        String.class.getName());
    assertThrows(IOException.class, () -> INodeStoreFactory.create(conf));
  }

  // -----------------------------------------------------------------------
  // InMemoryINodeStore CRUD tests
  // -----------------------------------------------------------------------

  @Test
  @Timeout(value = 10)
  public void testPutAndGet() throws Exception {
    assertTrue(store.isInitialized());

    INodeDirectory dir = makeDir(100L, "dir1");
    dir.setParent(null); // root-level
    store.put(dir);

    INode retrieved = store.get(100L);
    assertNotNull(retrieved);
    assertEquals(100L, retrieved.getId());
    assertTrue(retrieved.isDirectory());
  }

  @Test
  @Timeout(value = 10)
  public void testGetReturnsNullForMissingId() throws Exception {
    assertNull(store.get(9999L));
  }

  @Test
  @Timeout(value = 10)
  public void testRemove() throws Exception {
    INodeDirectory dir = makeDir(200L, "dir2");
    store.put(dir);
    assertNotNull(store.get(200L));

    store.remove(200L);
    assertNull(store.get(200L));
  }

  @Test
  @Timeout(value = 10)
  public void testSize() throws Exception {
    // root inode was already added in setUp; size == 1
    int before = store.size();

    store.put(makeDir(300L, "a"));
    store.put(makeDir(301L, "b"));
    assertEquals(before + 2, store.size());
  }

  @Test
  @Timeout(value = 10)
  public void testClear() throws Exception {
    store.put(makeDir(400L, "c"));
    store.put(makeDir(401L, "d"));

    store.clear();
    assertEquals(0, store.size());
  }

  @Test
  @Timeout(value = 10)
  public void testGetChildren() throws Exception {
    INodeDirectory parent = makeDir(500L, "parent");
    inodeMap.put(parent);

    INodeDirectory child1 = makeDir(501L, "a_child");
    INodeDirectory child2 = makeDir(502L, "b_child");
    child1.setParent(parent);
    child2.setParent(parent);
    parent.addChild(child1);
    parent.addChild(child2);

    List<INode> children = store.getChildren(500L);
    assertEquals(2, children.size());
  }

  @Test
  @Timeout(value = 10)
  public void testGetChild() throws Exception {
    INodeDirectory parent = makeDir(600L, "parent2");
    inodeMap.put(parent);

    INodeDirectory child = makeDir(601L, "target");
    child.setParent(parent);
    parent.addChild(child);

    INode found = store.getChild(600L, "target".getBytes());
    assertNotNull(found, "Should find child by name");
    assertEquals(601L, found.getId());
  }

  @Test
  @Timeout(value = 10)
  public void testGetChildReturnsNullWhenNotFound() throws Exception {
    INodeDirectory parent = makeDir(700L, "empty_dir");
    inodeMap.put(parent);

    assertNull(store.getChild(700L, "nonexistent".getBytes()));
  }

  // -----------------------------------------------------------------------
  // evictChildren / lazy-reload (InMemoryINodeStore path)
  // -----------------------------------------------------------------------

  /**
   * With an {@link InMemoryINodeStore} set as the active store,
   * {@link INodeDirectory#evictChildren()} should be a no-op because the
   * in-memory store does not support eviction.
   */
  @Test
  @Timeout(value = 10)
  public void testEvictChildrenIsNoOpForInMemoryStore() throws Exception {
    // Wire the static FSDirectory.iNodeStore to our InMemoryINodeStore so the
    // evictChildren() code path can check instanceof InMemoryINodeStore.
    FSDirectory.iNodeStore = store;
    try {
      INodeDirectory parent = makeDir(800L, "parent3");
      inodeMap.put(parent);

      INodeDirectory child = makeDir(801L, "child_e");
      child.setParent(parent);
      parent.addChild(child);

      parent.evictChildren(); // no-op for InMemoryINodeStore

      // children list is intact — eviction was skipped
      assertEquals(1,
          parent.getChildrenList(Snapshot.CURRENT_STATE_ID).size());
    } finally {
      FSDirectory.iNodeStore = null;
    }
  }

  /**
   * With no active store (null), {@link INodeDirectory#evictChildren()} should
   * also be a no-op, and children should remain accessible.
   */
  @Test
  @Timeout(value = 10)
  public void testEvictChildrenIsNoOpWithNullStore() throws Exception {
    // Ensure no store is active
    FSDirectory.iNodeStore = null;

    INodeDirectory parent = makeDir(900L, "parent4");
    inodeMap.put(parent);

    INodeDirectory child = makeDir(901L, "child_f");
    child.setParent(parent);
    parent.addChild(child);

    parent.evictChildren(); // should not throw; store is null

    assertEquals(1,
        parent.getChildrenList(Snapshot.CURRENT_STATE_ID).size());
  }

  // -----------------------------------------------------------------------
  // Config key constant
  // -----------------------------------------------------------------------

  @Test
  @Timeout(value = 10)
  public void testConfigKeyConstant() {
    assertEquals("dfs.namenode.inode.store.class",
        DFSConfigKeys.DFS_NAMENODE_INODE_STORE_CLASS_KEY);
  }

  // -----------------------------------------------------------------------
  // Helpers
  // -----------------------------------------------------------------------

  private static INodeDirectory makeDir(long id, String name) {
    return new INodeDirectory(id, name.getBytes(), PERMS, 0L);
  }
}
