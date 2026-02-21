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

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockInfo;
import org.apache.hadoop.hdfs.server.namenode.snapshot.DirectoryWithQuotaFeature;

/**
 * An {@link INodeStore} implementation that stores HDFS NameNode INode
 * metadata in a PostgreSQL database.
 *
 * <h2>Design goals</h2>
 * <ul>
 *   <li>Keep the HDFS client API (NameNode RPC) <em>unchanged</em>.
 *       PostgreSQL is the storage back-end; the NameNode code is the access
 *       layer.</li>
 *   <li>Allow the NameNode heap to remain nearly constant as the number of
 *       files grows.  Only the "hot" portion of the namespace needs to
 *       reside in heap at any time.</li>
 *   <li>Allow {@link INodeDirectory} children to be loaded lazily from
 *       PostgreSQL and evicted from memory when cold.</li>
 * </ul>
 *
 * <h2>Database schema</h2>
 * <pre>{@code
 * CREATE TABLE hdfs_inodes (
 *     id              BIGINT      PRIMARY KEY,
 *     parent_id       BIGINT,                    -- NULL for root inode
 *     local_name      BYTEA       NOT NULL DEFAULT ''::bytea,
 *     node_type       CHAR(1)     NOT NULL,      -- 'D' dir | 'F' file | 'S' symlink
 *     owner_name      TEXT        NOT NULL DEFAULT '',
 *     group_name      TEXT        NOT NULL DEFAULT '',
 *     perm_mode       SMALLINT    NOT NULL DEFAULT 0,
 *     mod_time        BIGINT      NOT NULL DEFAULT 0,
 *     access_time     BIGINT      NOT NULL DEFAULT 0,
 *     -- file-specific (nullable for non-files)
 *     replication     SMALLINT,
 *     block_size      BIGINT,
 *     storage_policy  SMALLINT    NOT NULL DEFAULT 0,
 *     -- dir-specific quotas
 *     ns_quota        BIGINT      NOT NULL DEFAULT -1,
 *     ds_quota        BIGINT      NOT NULL DEFAULT -1
 * );
 *
 * CREATE INDEX hdfs_inodes_parent ON hdfs_inodes(parent_id);
 * }</pre>
 *
 * <h2>Configuration</h2>
 * <pre>{@code
 * <!-- Use PostgreSQL as the INode store -->
 * <property>
 *   <name>dfs.namenode.inode.store.class</name>
 *   <value>org.apache.hadoop.hdfs.server.namenode.PostgreSQLINodeStore</value>
 * </property>
 * <property>
 *   <name>dfs.namenode.inode.store.pgsql.url</name>
 *   <value>jdbc:postgresql://db-host:5432/hadoop_meta</value>
 * </property>
 * <property>
 *   <name>dfs.namenode.inode.store.pgsql.username</name>
 *   <value>hdfs</value>
 * </property>
 * <property>
 *   <name>dfs.namenode.inode.store.pgsql.password</name>
 *   <value>secret</value>
 * </property>
 * }</pre>
 */
@InterfaceAudience.Private
public class PostgreSQLINodeStore implements INodeStore {

  private static final Logger LOG =
      LoggerFactory.getLogger(PostgreSQLINodeStore.class);

  // -----------------------------------------------------------------------
  // Configuration key constants
  // -----------------------------------------------------------------------

  /** Full JDBC URL for the PostgreSQL database, e.g.
   *  {@code jdbc:postgresql://db-host:5432/hadoop_meta}. */
  public static final String PGSQL_URL_KEY =
      "dfs.namenode.inode.store.pgsql.url";

  /** JDBC username. */
  public static final String PGSQL_USERNAME_KEY =
      "dfs.namenode.inode.store.pgsql.username";

  /** JDBC password (may be a Hadoop credential store alias). */
  public static final String PGSQL_PASSWORD_KEY =
      "dfs.namenode.inode.store.pgsql.password";

  // -----------------------------------------------------------------------
  // Table / column names
  // -----------------------------------------------------------------------

  static final String TABLE = "hdfs_inodes";

  /** node_type values */
  static final String TYPE_DIR     = "D";
  static final String TYPE_FILE    = "F";
  static final String TYPE_SYMLINK = "S";

  /** Sentinel value meaning "no parent" (root inode). */
  static final long NO_PARENT = -1L;

  // -----------------------------------------------------------------------
  // Instance state
  // -----------------------------------------------------------------------

  private final Configuration conf;
  private HikariDataSource dataSource;
  private volatile boolean initialized = false;

  // -----------------------------------------------------------------------
  // Constructor — called via reflection by INodeStoreFactory
  // -----------------------------------------------------------------------

  public PostgreSQLINodeStore(Configuration conf) {
    this.conf = conf;
  }

  // -----------------------------------------------------------------------
  // INodeStore implementation
  // -----------------------------------------------------------------------

  @Override
  public void initialize() throws IOException {
    String url = conf.get(PGSQL_URL_KEY, "");
    if (url.isEmpty()) {
      throw new IOException(PGSQL_URL_KEY + " is not configured");
    }

    String username = conf.get(PGSQL_USERNAME_KEY, "");
    String password = DFSUtil.getPassword(conf, PGSQL_PASSWORD_KEY);
    if (password == null) {
      password = "";
    }

    HikariConfig hc = new HikariConfig();
    hc.setJdbcUrl(url);
    hc.setDriverClassName("org.postgresql.Driver");
    hc.setUsername(username);
    hc.setPassword(password);
    // Auto-commit is appropriate here: every INode put/remove is an independent
    // idempotent upsert.  Durability of the namespace is guaranteed by the edit
    // log (which is the source of truth); the store is a read-optimised cache
    // that can be rebuilt from the edit log at any time.
    hc.setAutoCommit(true);
    hc.setMaximumPoolSize(20);
    hc.setConnectionTimeout(30_000);
    dataSource = new HikariDataSource(hc);

    ensureTableExists();
    initialized = true;
    LOG.info("PostgreSQLINodeStore initialized against {}", url);
  }

  @Override
  public boolean isInitialized() {
    return initialized;
  }

  @Override
  public void put(INode inode) throws IOException {
    checkInitialized();
    long parentId = (inode.getParent() != null)
        ? inode.getParent().getId() : NO_PARENT;

    // Use upsert (INSERT … ON CONFLICT DO UPDATE) to handle both creates
    // and updates with a single statement.
    String sql = "INSERT INTO " + TABLE + " ("
        + "id, parent_id, local_name, node_type, "
        + "owner_name, group_name, perm_mode, mod_time, access_time, "
        + "replication, block_size, storage_policy, ns_quota, ds_quota) "
        + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) "
        + "ON CONFLICT (id) DO UPDATE SET "
        + "  parent_id      = EXCLUDED.parent_id, "
        + "  local_name     = EXCLUDED.local_name, "
        + "  node_type      = EXCLUDED.node_type, "
        + "  owner_name     = EXCLUDED.owner_name, "
        + "  group_name     = EXCLUDED.group_name, "
        + "  perm_mode      = EXCLUDED.perm_mode, "
        + "  mod_time       = EXCLUDED.mod_time, "
        + "  access_time    = EXCLUDED.access_time, "
        + "  replication    = EXCLUDED.replication, "
        + "  block_size     = EXCLUDED.block_size, "
        + "  storage_policy = EXCLUDED.storage_policy, "
        + "  ns_quota       = EXCLUDED.ns_quota, "
        + "  ds_quota       = EXCLUDED.ds_quota";

    try (Connection conn = dataSource.getConnection();
         PreparedStatement ps = conn.prepareStatement(sql)) {

      ps.setLong(1, inode.getId());
      if (parentId == NO_PARENT) {
        ps.setNull(2, java.sql.Types.BIGINT);
      } else {
        ps.setLong(2, parentId);
      }
      ps.setBytes(3, inode.getLocalNameBytes());
      ps.setString(4, nodeType(inode));
      ps.setString(5, inode.getUserName());
      ps.setString(6, inode.getGroupName());
      ps.setShort(7, inode.getFsPermission().toShort());
      ps.setLong(8, inode.getModificationTime());
      ps.setLong(9, inode.getAccessTime());

      if (inode.isFile()) {
        INodeFile f = inode.asFile();
        ps.setShort(10, f.getFileReplication());
        ps.setLong(11, f.getPreferredBlockSize());
        ps.setShort(12, f.getLocalStoragePolicyID());
      } else {
        ps.setNull(10, java.sql.Types.SMALLINT);
        ps.setNull(11, java.sql.Types.BIGINT);
        ps.setShort(12, (short) 0);
      }

      if (inode.isDirectory()) {
        INodeDirectory dir = inode.asDirectory();
        DirectoryWithQuotaFeature quota = dir.getDirectoryWithQuotaFeature();
        if (quota != null) {
          ps.setLong(13, quota.getQuota().getNameSpace());
          ps.setLong(14, quota.getQuota().getStorageSpace());
        } else {
          ps.setLong(13, -1L);
          ps.setLong(14, -1L);
        }
      } else {
        ps.setLong(13, -1L);
        ps.setLong(14, -1L);
      }

      ps.executeUpdate();
    } catch (SQLException e) {
      throw new IOException("Failed to put inode id=" + inode.getId(), e);
    }
  }

  @Override
  public void remove(long inodeId) throws IOException {
    checkInitialized();
    String sql = "DELETE FROM " + TABLE + " WHERE id = ?";
    try (Connection conn = dataSource.getConnection();
         PreparedStatement ps = conn.prepareStatement(sql)) {
      ps.setLong(1, inodeId);
      ps.executeUpdate();
    } catch (SQLException e) {
      throw new IOException("Failed to remove inode id=" + inodeId, e);
    }
  }

  @Override
  public INode get(long inodeId) throws IOException {
    checkInitialized();
    String sql = "SELECT * FROM " + TABLE + " WHERE id = ?";
    try (Connection conn = dataSource.getConnection();
         PreparedStatement ps = conn.prepareStatement(sql)) {
      ps.setLong(1, inodeId);
      try (ResultSet rs = ps.executeQuery()) {
        if (rs.next()) {
          return buildINode(rs);
        }
      }
    } catch (SQLException e) {
      throw new IOException("Failed to get inode id=" + inodeId, e);
    }
    return null;
  }

  @Override
  public List<INode> getChildren(long parentId) throws IOException {
    checkInitialized();
    String sql = "SELECT * FROM " + TABLE
        + " WHERE parent_id = ?"
        + " ORDER BY local_name ASC";
    List<INode> children = new ArrayList<>();
    try (Connection conn = dataSource.getConnection();
         PreparedStatement ps = conn.prepareStatement(sql)) {
      ps.setLong(1, parentId);
      try (ResultSet rs = ps.executeQuery()) {
        while (rs.next()) {
          children.add(buildINode(rs));
        }
      }
    } catch (SQLException e) {
      throw new IOException(
          "Failed to get children of parentId=" + parentId, e);
    }
    return children;
  }

  @Override
  public INode getChild(long parentId, byte[] localName) throws IOException {
    checkInitialized();
    String sql = "SELECT * FROM " + TABLE
        + " WHERE parent_id = ? AND local_name = ?";
    try (Connection conn = dataSource.getConnection();
         PreparedStatement ps = conn.prepareStatement(sql)) {
      ps.setLong(1, parentId);
      ps.setBytes(2, localName);
      try (ResultSet rs = ps.executeQuery()) {
        if (rs.next()) {
          return buildINode(rs);
        }
      }
    } catch (SQLException e) {
      throw new IOException(
          "Failed to get child of parentId=" + parentId
              + " name=" + Arrays.toString(localName), e);
    }
    return null;
  }

  @Override
  public int size() throws IOException {
    checkInitialized();
    String sql = "SELECT COUNT(*) FROM " + TABLE;
    try (Connection conn = dataSource.getConnection();
         Statement st = conn.createStatement();
         ResultSet rs = st.executeQuery(sql)) {
      rs.next();
      return rs.getInt(1);
    } catch (SQLException e) {
      throw new IOException("Failed to count inodes", e);
    }
  }

  @Override
  public void clear() throws IOException {
    checkInitialized();
    // TRUNCATE is used for performance on the assumption that no foreign keys
    // reference hdfs_inodes from outside this table.  If CASCADE is needed,
    // replace with "TRUNCATE TABLE hdfs_inodes CASCADE".
    String sql = "TRUNCATE TABLE " + TABLE;
    try (Connection conn = dataSource.getConnection();
         Statement st = conn.createStatement()) {
      st.execute(sql);
    } catch (SQLException e) {
      throw new IOException("Failed to clear inode store", e);
    }
  }

  @Override
  public Iterator<INodeWithAdditionalFields> iterator() throws IOException {
    checkInitialized();
    // Fetch all rows into a list so we don't hold a long-lived cursor open.
    List<INodeWithAdditionalFields> result = new ArrayList<>();
    String sql = "SELECT * FROM " + TABLE + " ORDER BY id ASC";
    try (Connection conn = dataSource.getConnection();
         Statement st = conn.createStatement();
         ResultSet rs = st.executeQuery(sql)) {
      while (rs.next()) {
        INode n = buildINode(rs);
        if (n instanceof INodeWithAdditionalFields) {
          result.add((INodeWithAdditionalFields) n);
        }
      }
    } catch (SQLException e) {
      throw new IOException("Failed to iterate inodes", e);
    }
    return result.iterator();
  }

  @Override
  public void close() throws IOException {
    if (dataSource != null && !dataSource.isClosed()) {
      dataSource.close();
      LOG.info("PostgreSQLINodeStore closed");
    }
    initialized = false;
  }

  // -----------------------------------------------------------------------
  // Package-private helper — exposed for testing
  // -----------------------------------------------------------------------

  /** Set the data source directly (for unit tests that inject a mock). */
  void setDataSource(HikariDataSource ds) throws IOException {
    this.dataSource = ds;
    ensureTableExists();
    this.initialized = true;
  }

  // -----------------------------------------------------------------------
  // Private helpers
  // -----------------------------------------------------------------------

  private void checkInitialized() throws IOException {
    if (!initialized) {
      throw new IOException("PostgreSQLINodeStore is not initialized");
    }
  }

  private void ensureTableExists() throws IOException {
    String createTable = "CREATE TABLE IF NOT EXISTS " + TABLE + " ("
        + "  id              BIGINT      NOT NULL PRIMARY KEY,"
        + "  parent_id       BIGINT,"
        + "  local_name      BYTEA       NOT NULL DEFAULT ''::bytea,"
        + "  node_type       CHAR(1)     NOT NULL,"
        + "  owner_name      TEXT        NOT NULL DEFAULT '',"
        + "  group_name      TEXT        NOT NULL DEFAULT '',"
        + "  perm_mode       SMALLINT    NOT NULL DEFAULT 0,"
        + "  mod_time        BIGINT      NOT NULL DEFAULT 0,"
        + "  access_time     BIGINT      NOT NULL DEFAULT 0,"
        + "  replication     SMALLINT,"
        + "  block_size      BIGINT,"
        + "  storage_policy  SMALLINT    NOT NULL DEFAULT 0,"
        + "  ns_quota        BIGINT      NOT NULL DEFAULT -1,"
        + "  ds_quota        BIGINT      NOT NULL DEFAULT -1"
        + ")";

    String createIndex =
        "CREATE INDEX IF NOT EXISTS hdfs_inodes_parent"
        + " ON " + TABLE + "(parent_id)";

    try (Connection conn = dataSource.getConnection();
         Statement st = conn.createStatement()) {
      st.execute(createTable);
      st.execute(createIndex);
    } catch (SQLException e) {
      throw new IOException("Failed to create hdfs_inodes table", e);
    }
  }

  /**
   * Reconstruct an {@link INode} from a {@link ResultSet} row.
   *
   * <p>Block data is NOT stored in this table (blocks are managed by
   * {@link org.apache.hadoop.hdfs.server.blockmanagement.BlockManager}).
   * {@link INodeFile} objects returned here have an empty block array;
   * the block array is populated by the block report processing logic
   * when the DataNodes contact the NameNode.
   */
  private INode buildINode(ResultSet rs) throws SQLException {
    long id          = rs.getLong("id");
    byte[] name      = rs.getBytes("local_name");
    if (name == null) {
      name = new byte[0];
    }
    String type      = rs.getString("node_type");
    String owner     = rs.getString("owner_name");
    String group     = rs.getString("group_name");
    short permMode   = rs.getShort("perm_mode");
    long modTime     = rs.getLong("mod_time");
    long accessTime  = rs.getLong("access_time");

    PermissionStatus perms = new PermissionStatus(
        owner, group, FsPermission.createImmutable(permMode));

    if (TYPE_DIR.equals(type)) {
      long nsQuota = rs.getLong("ns_quota");
      long dsQuota = rs.getLong("ds_quota");
      INodeDirectory dir = new INodeDirectory(id, name, perms, modTime);
      if (nsQuota >= 0 || dsQuota >= 0) {
        dir.addDirectoryWithQuotaFeature(
            new DirectoryWithQuotaFeature.Builder()
                .nameSpaceQuota(nsQuota)
                .storageSpaceQuota(dsQuota)
                .build());
      }
      return dir;

    } else if (TYPE_FILE.equals(type)) {
      short replication = rs.getShort("replication");
      long blockSize    = rs.getLong("block_size");
      // Blocks are managed by BlockManager — return file with empty block array.
      return new INodeFile(id, name, perms, modTime, accessTime,
          BlockInfo.EMPTY_ARRAY, replication, blockSize);

    } else {
      // SYMLINK: requires a symlink_target BYTEA column (not yet implemented).
      // Return null to signal to the caller that symlinks are not supported;
      // a full implementation would add a symlink_target column and store the
      // DFSUtil.bytes2String(n.getSymlink()) value.
      LOG.warn("Symlink inode id={} cannot be loaded from PostgreSQLINodeStore:"
          + " symlink_target storage is not yet implemented", id);
      return null;
    }
  }

  private static String nodeType(INode inode) {
    if (inode.isDirectory()) return TYPE_DIR;
    if (inode.isFile())      return TYPE_FILE;
    return TYPE_SYMLINK;
  }
}
