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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience;

/**
 * An implementation of {@link EditLogOutputStream} that writes edit log
 * operations to a PostgreSQL database.
 *
 * <p>Each log segment is stored as a row in the {@code hdfs_editlog_segments}
 * table. Bytes are appended to the {@code data} BYTEA column on every flush
 * using PostgreSQL's {@code ||} concatenation operator, which avoids reading
 * the full segment back from the database on every write.
 */
@InterfaceAudience.Private
class PostgreSQLEditLogOutputStream extends EditLogOutputStream {

  private static final Logger LOG =
      LoggerFactory.getLogger(PostgreSQLEditLogOutputStream.class);

  /** The connection used exclusively by this output stream. */
  private final Connection connection;

  /** Transaction ID of the first op in this segment. */
  private final long firstTxId;

  /** Double-buffer that accumulates ops between flushes. */
  private final EditsDoubleBuffer doubleBuf;

  /**
   * Staging area: the ready half of the double-buffer is flushed here before
   * being written to PostgreSQL in a single {@code UPDATE}.
   */
  private final ByteArrayOutputStream stagingBuf = new ByteArrayOutputStream(512 * 1024);

  private boolean closed = false;

  PostgreSQLEditLogOutputStream(Connection connection, long firstTxId,
      int bufferCapacity) throws IOException {
    super();
    this.connection = connection;
    this.firstTxId = firstTxId;
    this.doubleBuf = new EditsDoubleBuffer(bufferCapacity);
  }

  // -----------------------------------------------------------------------
  // EditLogOutputStream implementation
  // -----------------------------------------------------------------------

  @Override
  public void write(FSEditLogOp op) throws IOException {
    doubleBuf.writeOp(op, getCurrentLogVersion());
  }

  @Override
  public void writeRaw(byte[] bytes, int offset, int length)
      throws IOException {
    doubleBuf.writeRaw(bytes, offset, length);
  }

  /**
   * Initialise a new segment row in the database and write the edit-log
   * header (layout version + layout flags).
   */
  @Override
  public void create(int layoutVersion) throws IOException {
    setCurrentLogVersion(layoutVersion);
    // Write the header into the current (writable) side of the double buffer.
    EditLogFileOutputStream.writeHeader(layoutVersion,
        doubleBuf.getCurrentBuf());
    setReadyToFlush();
    flush();
  }

  @Override
  public void setReadyToFlush() throws IOException {
    doubleBuf.setReadyToFlush();
  }

  @Override
  protected void flushAndSync(boolean durable) throws IOException {
    if (closed) {
      throw new IOException("Stream for segment " + firstTxId + " is closed");
    }
    if (doubleBuf.isFlushed()) {
      return;
    }

    // Drain the ready buffer into our staging ByteArrayOutputStream.
    stagingBuf.reset();
    doubleBuf.flushTo(stagingBuf);

    byte[] bytes = stagingBuf.toByteArray();
    if (bytes.length == 0) {
      return;
    }

    // Append new bytes to the existing BYTEA column.
    String sql = "UPDATE hdfs_editlog_segments"
        + " SET data = data || ?"
        + " WHERE first_tx_id = ?";
    try (PreparedStatement ps = connection.prepareStatement(sql)) {
      ps.setBytes(1, bytes);
      ps.setLong(2, firstTxId);
      int rows = ps.executeUpdate();
      if (rows == 0) {
        throw new IOException(
            "No segment row found for firstTxId=" + firstTxId
                + "; cannot append edit log data");
      }
      if (!connection.getAutoCommit()) {
        connection.commit();
      }
    } catch (SQLException e) {
      throw new IOException(
          "Failed to flush edit log data for segment " + firstTxId, e);
    }
  }

  @Override
  public void close() throws IOException {
    if (closed) {
      return;
    }
    try {
      // Flush any remaining buffered data.
      if (doubleBuf != null) {
        doubleBuf.close();
      }
    } finally {
      closed = true;
      try {
        connection.close();
      } catch (SQLException e) {
        LOG.warn("Failed to close PostgreSQL connection for segment {}",
            firstTxId, e);
      }
    }
  }

  @Override
  public void abort() throws IOException {
    if (closed) {
      return;
    }
    closed = true;
    try {
      connection.close();
    } catch (SQLException e) {
      LOG.warn("Failed to close PostgreSQL connection on abort for segment {}",
          firstTxId, e);
    }
  }

  @Override
  public boolean shouldForceSync() {
    return doubleBuf.shouldForceSync();
  }

  @Override
  public String toString() {
    return "PostgreSQLEditLogOutputStream(firstTxId=" + firstTxId + ")";
  }
}
