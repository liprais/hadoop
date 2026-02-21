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
import java.net.URI;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.common.Storage;
import org.apache.hadoop.hdfs.server.common.StorageInfo;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;

/**
 * A {@link JournalManager} implementation that stores HDFS NameNode edit
 * logs in a PostgreSQL database.
 *
 * <h2>Database schema</h2>
 * <pre>{@code
 * CREATE TABLE hdfs_editlog_segments (
 *     first_tx_id   BIGINT  PRIMARY KEY,
 *     last_tx_id    BIGINT  NOT NULL DEFAULT -1,
 *     in_progress   BOOLEAN NOT NULL DEFAULT TRUE,
 *     layout_version INT    NOT NULL,
 *     data          BYTEA   NOT NULL DEFAULT ''::bytea
 * );
 *
 * CREATE TABLE hdfs_namespace_info (
 *     id             SMALLINT PRIMARY KEY DEFAULT 1,
 *     namespace_id   INT     NOT NULL,
 *     cluster_id     VARCHAR(255) NOT NULL,
 *     block_pool_id  VARCHAR(255) NOT NULL,
 *     layout_version INT     NOT NULL,
 *     c_time         BIGINT  NOT NULL
 * );
 * }</pre>
 *
 * <h2>Configuration</h2>
 * <pre>{@code
 * <!-- Register the plugin for the "pgsql" URI scheme -->
 * <property>
 *   <name>dfs.namenode.edits.journal-plugin.pgsql</name>
 *   <value>org.apache.hadoop.hdfs.server.namenode.PostgreSQLJournalManager</value>
 * </property>
 *
 * <!-- Point the edit-log at your PostgreSQL instance -->
 * <property>
 *   <name>dfs.namenode.edits.dir</name>
 *   <value>pgsql://db-host:5432/hadoop_meta</value>
 * </property>
 *
 * <!-- Database credentials (password may come from a Hadoop credential store) -->
 * <property>
 *   <name>dfs.namenode.edits.pgsql.username</name>
 *   <value>hdfs</value>
 * </property>
 * <property>
 *   <name>dfs.namenode.edits.pgsql.password</name>
 *   <value>secret</value>
 * </property>
 * }</pre>
 */
@InterfaceAudience.Private
public class PostgreSQLJournalManager implements JournalManager {

  private static final Logger LOG =
      LoggerFactory.getLogger(PostgreSQLJournalManager.class);

  // -----------------------------------------------------------------------
  // Configuration key constants
  // -----------------------------------------------------------------------

  /**
   * JDBC username for the PostgreSQL edit-log database.
   * Example: {@code hdfs}.
   */
  public static final String PGSQL_USERNAME_KEY =
      "dfs.namenode.edits.pgsql.username";

  /**
   * JDBC password (or Hadoop credential-store alias) for the PostgreSQL
   * edit-log database.
   */
  public static final String PGSQL_PASSWORD_KEY =
      "dfs.namenode.edits.pgsql.password";

  // -----------------------------------------------------------------------
  // Table / column names
  // -----------------------------------------------------------------------

  static final String TBL_SEGMENTS  = "hdfs_editlog_segments";
  static final String TBL_NAMESPACE = "hdfs_namespace_info";

  // -----------------------------------------------------------------------
  // Instance state
  // -----------------------------------------------------------------------

  private final Configuration conf;
  private final URI uri;
  private final NamespaceInfo nsInfo;
  private final HikariDataSource dataSource;

  private int outputBufferCapacity = 512 * 1024;

  // -----------------------------------------------------------------------
  // Constructor (called by FSEditLog.createJournal via reflection)
  // -----------------------------------------------------------------------

  public PostgreSQLJournalManager(Configuration conf, URI uri,
      NamespaceInfo nsInfo) {
    this.conf = conf;
    this.uri = uri;
    this.nsInfo = nsInfo;
    this.dataSource = buildDataSource(conf, uri);
    LOG.info("PostgreSQLJournalManager initialised for {}", uri);
  }

  // -----------------------------------------------------------------------
  // Internal helpers
  // -----------------------------------------------------------------------

  /**
   * Derives the JDBC URL from the plugin URI and creates a HikariCP pool.
   *
   * <p>A {@code pgsql://db-host:5432/hadoop_meta} URI becomes
   * {@code jdbc:postgresql://db-host:5432/hadoop_meta}.
   */
  private static HikariDataSource buildDataSource(Configuration conf,
      URI uri) {
    String jdbcUrl = "jdbc:postgresql://"
        + uri.getHost()
        + (uri.getPort() > 0 ? ":" + uri.getPort() : "")
        + uri.getPath();

    String username = conf.get(PGSQL_USERNAME_KEY, "");
    String password = DFSUtil.getPassword(conf, PGSQL_PASSWORD_KEY);
    if (password == null) {
      password = "";
    }

    HikariConfig hc = new HikariConfig();
    hc.setJdbcUrl(jdbcUrl);
    hc.setDriverClassName("org.postgresql.Driver");
    hc.setUsername(username);
    hc.setPassword(password);
    hc.setAutoCommit(true);
    hc.setMaximumPoolSize(10);
    hc.setConnectionTimeout(30_000);
    return new HikariDataSource(hc);
  }

  /** Returns a fresh {@link Connection} from the pool. */
  private Connection getConnection() throws IOException {
    try {
      return dataSource.getConnection();
    } catch (SQLException e) {
      throw new IOException("Cannot obtain PostgreSQL connection", e);
    }
  }

  /**
   * Creates the two metadata tables if they do not already exist.
   */
  private void ensureTablesExist() throws IOException {
    String createSegments =
        "CREATE TABLE IF NOT EXISTS " + TBL_SEGMENTS + " ("
        + "  first_tx_id    BIGINT  PRIMARY KEY,"
        + "  last_tx_id     BIGINT  NOT NULL DEFAULT -1,"
        + "  in_progress    BOOLEAN NOT NULL DEFAULT TRUE,"
        + "  layout_version INT     NOT NULL,"
        + "  data           BYTEA   NOT NULL DEFAULT ''::bytea"
        + ")";

    String createNamespace =
        "CREATE TABLE IF NOT EXISTS " + TBL_NAMESPACE + " ("
        + "  id             SMALLINT PRIMARY KEY DEFAULT 1,"
        + "  namespace_id   INT     NOT NULL,"
        + "  cluster_id     VARCHAR(255) NOT NULL,"
        + "  block_pool_id  VARCHAR(255) NOT NULL,"
        + "  layout_version INT     NOT NULL,"
        + "  c_time         BIGINT  NOT NULL"
        + ")";

    try (Connection conn = getConnection();
         Statement st = conn.createStatement()) {
      st.execute(createSegments);
      st.execute(createNamespace);
    } catch (SQLException e) {
      throw new IOException("Failed to create edit-log tables", e);
    }
  }

  // -----------------------------------------------------------------------
  // JournalManager implementation
  // -----------------------------------------------------------------------

  @Override
  public void format(NamespaceInfo ns, boolean force) throws IOException {
    ensureTablesExist();

    try (Connection conn = getConnection();
         Statement st = conn.createStatement()) {
      st.execute("TRUNCATE TABLE " + TBL_SEGMENTS);
      st.execute("TRUNCATE TABLE " + TBL_NAMESPACE);

      String upsert =
          "INSERT INTO " + TBL_NAMESPACE
          + " (id, namespace_id, cluster_id, block_pool_id,"
          + "  layout_version, c_time)"
          + " VALUES (1, ?, ?, ?, ?, ?)"
          + " ON CONFLICT (id) DO UPDATE SET"
          + "   namespace_id   = EXCLUDED.namespace_id,"
          + "   cluster_id     = EXCLUDED.cluster_id,"
          + "   block_pool_id  = EXCLUDED.block_pool_id,"
          + "   layout_version = EXCLUDED.layout_version,"
          + "   c_time         = EXCLUDED.c_time";
      try (PreparedStatement ps = conn.prepareStatement(upsert)) {
        ps.setInt(1, ns.getNamespaceID());
        ps.setString(2, ns.getClusterID());
        ps.setString(3, ns.getBlockPoolID());
        ps.setInt(4, ns.getLayoutVersion());
        ps.setLong(5, ns.getCTime());
        ps.executeUpdate();
      }
    } catch (SQLException e) {
      throw new IOException("Failed to format PostgreSQL journal", e);
    }
    LOG.info("PostgreSQL journal formatted for namespace {}", ns.getNamespaceID());
  }

  @Override
  public boolean hasSomeData() throws IOException {
    ensureTablesExist();
    try (Connection conn = getConnection();
         Statement st = conn.createStatement();
         ResultSet rs = st.executeQuery(
             "SELECT 1 FROM " + TBL_NAMESPACE + " LIMIT 1")) {
      return rs.next();
    } catch (SQLException e) {
      throw new IOException("Failed to check for existing data", e);
    }
  }

  @Override
  public EditLogOutputStream startLogSegment(long txId, int layoutVersion)
      throws IOException {
    ensureTablesExist();

    // Insert a new (empty) segment row.
    String insert =
        "INSERT INTO " + TBL_SEGMENTS
        + " (first_tx_id, last_tx_id, in_progress, layout_version, data)"
        + " VALUES (?, -1, TRUE, ?, ''::bytea)";
    try (Connection conn = getConnection();
         PreparedStatement ps = conn.prepareStatement(insert)) {
      ps.setLong(1, txId);
      ps.setInt(2, layoutVersion);
      ps.executeUpdate();
    } catch (SQLException e) {
      throw new IOException(
          "Failed to insert edit-log segment row for txId=" + txId, e);
    }

    // Open a dedicated connection for the output stream so that concurrent
    // reads can still use the pool.
    Connection streamConn = getConnection();
    PostgreSQLEditLogOutputStream stream =
        new PostgreSQLEditLogOutputStream(streamConn, txId,
            outputBufferCapacity);
    stream.create(layoutVersion);
    return stream;
  }

  @Override
  public void finalizeLogSegment(long firstTxId, long lastTxId)
      throws IOException {
    String sql =
        "UPDATE " + TBL_SEGMENTS
        + " SET in_progress = FALSE, last_tx_id = ?"
        + " WHERE first_tx_id = ?";
    try (Connection conn = getConnection();
         PreparedStatement ps = conn.prepareStatement(sql)) {
      ps.setLong(1, lastTxId);
      ps.setLong(2, firstTxId);
      int rows = ps.executeUpdate();
      if (rows == 0) {
        LOG.warn("finalizeLogSegment: no segment row found for firstTxId={}",
            firstTxId);
      }
    } catch (SQLException e) {
      throw new IOException(
          "Failed to finalize segment firstTxId=" + firstTxId, e);
    }
  }

  @Override
  public void setOutputBufferCapacity(int size) {
    this.outputBufferCapacity = size;
  }

  @Override
  public void selectInputStreams(Collection<EditLogInputStream> streams,
      long fromTxId, boolean inProgressOk, boolean onlyDurableTxns)
      throws IOException {
    String sql =
        "SELECT first_tx_id, last_tx_id, in_progress, data"
        + " FROM " + TBL_SEGMENTS
        + " WHERE last_tx_id >= ? OR in_progress = TRUE"
        + " ORDER BY first_tx_id ASC";

    try (Connection conn = getConnection();
         PreparedStatement ps = conn.prepareStatement(sql)) {
      ps.setLong(1, fromTxId);
      try (ResultSet rs = ps.executeQuery()) {
        while (rs.next()) {
          long segFirstTxId  = rs.getLong("first_tx_id");
          long segLastTxId   = rs.getLong("last_tx_id");
          boolean segInProg  = rs.getBoolean("in_progress");
          byte[] data        = rs.getBytes("data");

          if (segInProg && !inProgressOk) {
            continue;
          }

          // Skip segments that end before the requested starting txId.
          if (!segInProg
              && segLastTxId != HdfsServerConstants.INVALID_TXID
              && segLastTxId < fromTxId) {
            continue;
          }

          String segName = String.format(
              "PostgreSQLEditLog[%d,%s]",
              segFirstTxId,
              segInProg ? "inProgress" : Long.toString(segLastTxId));

          streams.add(new PostgreSQLEditLogInputStream(
              segName, data, segFirstTxId, segLastTxId, segInProg));
        }
      }
    } catch (SQLException e) {
      throw new IOException("Failed to select edit-log input streams", e);
    }
  }

  @Override
  public void purgeLogsOlderThan(long minTxIdToKeep) throws IOException {
    String sql =
        "DELETE FROM " + TBL_SEGMENTS
        + " WHERE in_progress = FALSE AND last_tx_id < ?";
    try (Connection conn = getConnection();
         PreparedStatement ps = conn.prepareStatement(sql)) {
      ps.setLong(1, minTxIdToKeep);
      int rows = ps.executeUpdate();
      LOG.info("purgeLogsOlderThan({}): deleted {} segment(s)",
          minTxIdToKeep, rows);
    } catch (SQLException e) {
      throw new IOException(
          "Failed to purge edit-log segments older than " + minTxIdToKeep, e);
    }
  }

  @Override
  public void recoverUnfinalizedSegments() throws IOException {
    // Collect all in-progress segments.
    List<long[]> inProgress = new ArrayList<>();
    String querySql =
        "SELECT first_tx_id, data FROM " + TBL_SEGMENTS
        + " WHERE in_progress = TRUE ORDER BY first_tx_id ASC";

    try (Connection conn = getConnection();
         PreparedStatement ps = conn.prepareStatement(querySql);
         ResultSet rs = ps.executeQuery()) {
      while (rs.next()) {
        long segFirstTxId = rs.getLong("first_tx_id");
        byte[] data = rs.getBytes("data");

        // Scan the stored bytes to find the actual last txId.
        long lastTxId = scanLastTxId(segFirstTxId, data);
        if (lastTxId == HdfsServerConstants.INVALID_TXID) {
          // No valid transactions – delete the empty/corrupt segment.
          deleteSegment(segFirstTxId);
          LOG.info("Deleted empty/corrupt in-progress segment firstTxId={}",
              segFirstTxId);
        } else {
          inProgress.add(new long[]{segFirstTxId, lastTxId});
        }
      }
    } catch (SQLException e) {
      throw new IOException("Failed to recover unfinalized segments", e);
    }

    // Finalize any segments that had at least one transaction.
    for (long[] seg : inProgress) {
      finalizeLogSegment(seg[0], seg[1]);
      LOG.info("Recovered segment: firstTxId={}, lastTxId={}", seg[0], seg[1]);
    }
  }

  // -----------------------------------------------------------------------
  // Upgrade / rollback lifecycle (no-op for initial PoC)
  // -----------------------------------------------------------------------

  @Override
  public void doPreUpgrade() throws IOException {
    LOG.info("doPreUpgrade: no-op for PostgreSQLJournalManager");
  }

  @Override
  public void doUpgrade(Storage storage) throws IOException {
    LOG.info("doUpgrade: no-op for PostgreSQLJournalManager");
  }

  @Override
  public void doFinalize() throws IOException {
    LOG.info("doFinalize: no-op for PostgreSQLJournalManager");
  }

  @Override
  public boolean canRollBack(StorageInfo storage, StorageInfo prevStorage,
      int targetLayoutVersion) throws IOException {
    return false;
  }

  @Override
  public void doRollback() throws IOException {
    LOG.info("doRollback: no-op for PostgreSQLJournalManager");
  }

  @Override
  public void discardSegments(long startTxId) throws IOException {
    String sql =
        "DELETE FROM " + TBL_SEGMENTS
        + " WHERE first_tx_id >= ?";
    try (Connection conn = getConnection();
         PreparedStatement ps = conn.prepareStatement(sql)) {
      ps.setLong(1, startTxId);
      int rows = ps.executeUpdate();
      LOG.info("discardSegments(startTxId={}): deleted {} segment(s)",
          startTxId, rows);
    } catch (SQLException e) {
      throw new IOException(
          "Failed to discard segments from txId=" + startTxId, e);
    }
  }

  @Override
  public long getJournalCTime() throws IOException {
    String sql = "SELECT c_time FROM " + TBL_NAMESPACE + " WHERE id = 1";
    try (Connection conn = getConnection();
         Statement st = conn.createStatement();
         ResultSet rs = st.executeQuery(sql)) {
      if (rs.next()) {
        return rs.getLong("c_time");
      }
      return 0L;
    } catch (SQLException e) {
      throw new IOException("Failed to read journal c_time", e);
    }
  }

  @Override
  public void close() throws IOException {
    if (dataSource != null && !dataSource.isClosed()) {
      dataSource.close();
      LOG.info("PostgreSQLJournalManager closed for {}", uri);
    }
  }

  // -----------------------------------------------------------------------
  // Private helpers
  // -----------------------------------------------------------------------

  /**
   * Scans the bytes of an edit-log segment to find the last valid transaction
   * ID. Returns {@link HdfsServerConstants#INVALID_TXID} if no ops are found.
   */
  private long scanLastTxId(long segFirstTxId, byte[] data) {
    if (data == null || data.length == 0) {
      return HdfsServerConstants.INVALID_TXID;
    }
    try {
      PostgreSQLEditLogInputStream in = new PostgreSQLEditLogInputStream(
          "recovery-scan-" + segFirstTxId, data,
          segFirstTxId, HdfsServerConstants.INVALID_TXID, true);
      FSEditLogLoader.EditLogValidation val =
          FSEditLogLoader.scanEditLog(in, Long.MAX_VALUE);
      return val.getEndTxId();
    } catch (IOException e) {
      LOG.warn("Failed to scan in-progress segment firstTxId={}", segFirstTxId, e);
      return HdfsServerConstants.INVALID_TXID;
    }
  }

  /**
   * Removes a segment row from the database.
   */
  private void deleteSegment(long firstTxId) throws IOException {
    String sql = "DELETE FROM " + TBL_SEGMENTS + " WHERE first_tx_id = ?";
    try (Connection conn = getConnection();
         PreparedStatement ps = conn.prepareStatement(sql)) {
      ps.setLong(1, firstTxId);
      ps.executeUpdate();
    } catch (SQLException e) {
      throw new IOException(
          "Failed to delete segment firstTxId=" + firstTxId, e);
    }
  }

  // -----------------------------------------------------------------------
  // Overrides
  // -----------------------------------------------------------------------

  @Override
  public String toString() {
    return "PostgreSQLJournalManager(" + uri + ")";
  }
}
