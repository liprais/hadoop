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
import java.lang.reflect.Constructor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;

/**
 * Factory for creating the configured {@link INodeStore} implementation.
 *
 * <p>The implementation class is chosen by the configuration key
 * {@value DFSConfigKeys#DFS_NAMENODE_INODE_STORE_CLASS_KEY}.  When the key is
 * absent or empty the default in-memory implementation is used and the
 * NameNode behaves exactly as before.
 *
 * <p>Any class specified must:
 * <ul>
 *   <li>implement {@link INodeStore}, and</li>
 *   <li>have a public constructor that accepts a single
 *       {@link Configuration} argument.</li>
 * </ul>
 */
@InterfaceAudience.Private
public final class INodeStoreFactory {

  private static final Logger LOG =
      LoggerFactory.getLogger(INodeStoreFactory.class);

  private INodeStoreFactory() {}

  /**
   * Create and initialise an {@link INodeStore} instance based on the
   * current configuration.
   *
   * <p>If {@code dfs.namenode.inode.store.class} is not set, or is set to
   * the in-memory implementation, an {@link InMemoryINodeStore} is returned
   * (the default behaviour, fully backward-compatible).
   *
   * @param conf Hadoop configuration
   * @return an initialised {@link INodeStore}, never {@code null}
   * @throws IOException if the configured class cannot be instantiated or
   *                     initialised
   */
  public static INodeStore create(Configuration conf) throws IOException {
    String className = conf.get(
        DFSConfigKeys.DFS_NAMENODE_INODE_STORE_CLASS_KEY, "").trim();

    if (className.isEmpty()
        || InMemoryINodeStore.class.getName().equals(className)) {
      LOG.debug("Using in-memory INodeStore (default)");
      InMemoryINodeStore inMemory = new InMemoryINodeStore();
      inMemory.initialize();
      return inMemory;
    }

    LOG.info("Creating INodeStore: {}", className);
    try {
      Class<?> clazz = Class.forName(className);
      if (!INodeStore.class.isAssignableFrom(clazz)) {
        throw new IOException(className + " does not implement INodeStore");
      }
      Constructor<?> ctor = clazz.getConstructor(Configuration.class);
      INodeStore store = (INodeStore) ctor.newInstance(conf);
      store.initialize();
      return store;
    } catch (ReflectiveOperationException e) {
      throw new IOException(
          "Cannot instantiate INodeStore class: " + className, e);
    }
  }
}
