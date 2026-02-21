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

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.protocol.LayoutFlags;
import org.apache.hadoop.hdfs.protocol.LayoutVersion;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;

/**
 * An implementation of {@link EditLogInputStream} that reads edit log
 * operations from bytes previously fetched from a PostgreSQL database.
 *
 * <p>The raw bytes stored in the {@code hdfs_editlog_segments.data} column are
 * identical in format to the bytes stored in a file-based edit log. They
 * start with the layout-version header written by
 * {@link EditLogFileOutputStream#writeHeader} and are followed by serialised
 * {@link FSEditLogOp} records.
 */
@InterfaceAudience.Private
class PostgreSQLEditLogInputStream extends EditLogInputStream {

  private static final Logger LOG =
      LoggerFactory.getLogger(PostgreSQLEditLogInputStream.class);

  private final String name;
  private final long firstTxId;
  private final long lastTxId;
  private final boolean inProgress;
  private final byte[] data;

  private int maxOpSize = DFSConfigKeys.DFS_NAMENODE_MAX_OP_SIZE_DEFAULT;

  // Initialised lazily on the first read.
  private enum State { UNINIT, OPEN, CLOSED }
  private State state = State.UNINIT;

  private int logVersion = 0;
  private FSEditLogOp.Reader reader = null;
  private FSEditLogLoader.PositionTrackingInputStream tracker = null;

  /**
   * Creates a new input stream backed by an in-memory byte array.
   *
   * @param name       human-readable name used in log messages
   * @param data       raw bytes of the edit log segment (including header)
   * @param firstTxId  first transaction ID in this segment
   * @param lastTxId   last transaction ID in this segment; use
   *                   {@link HdfsServerConstants#INVALID_TXID} for in-progress
   * @param inProgress {@code true} if the segment has not been finalized
   */
  PostgreSQLEditLogInputStream(String name, byte[] data,
      long firstTxId, long lastTxId, boolean inProgress) {
    this.name = name;
    this.data = data;
    this.firstTxId = firstTxId;
    this.lastTxId = lastTxId;
    this.inProgress = inProgress;
  }

  // -----------------------------------------------------------------------
  // Lazy initialisation
  // -----------------------------------------------------------------------

  private void init() throws EditLogFileInputStream.LogHeaderCorruptException,
      IOException {
    if (state != State.UNINIT) {
      return;
    }
    try {
      BufferedInputStream bin =
          new BufferedInputStream(new ByteArrayInputStream(data));
      tracker = new FSEditLogLoader.PositionTrackingInputStream(bin);
      DataInputStream dataIn = new DataInputStream(tracker);

      // Read and validate the layout-version header.
      try {
        logVersion = dataIn.readInt();
      } catch (EOFException e) {
        throw new EditLogFileInputStream.LogHeaderCorruptException(
            "No header found in PostgreSQL edit log segment " + name);
      }

      if (logVersion == -1) {
        throw new EditLogFileInputStream.LogHeaderCorruptException(
            "Header value is -1 in segment " + name
                + "; segment appears empty/corrupt");
      }

      if (NameNodeLayoutVersion.supports(
              LayoutVersion.Feature.ADD_LAYOUT_FLAGS, logVersion)
          || logVersion < NameNodeLayoutVersion.CURRENT_LAYOUT_VERSION) {
        try {
          LayoutFlags.read(dataIn);
        } catch (EOFException e) {
          throw new EditLogFileInputStream.LogHeaderCorruptException(
              "EOF while reading layout flags from segment " + name);
        }
      }

      reader = FSEditLogOp.Reader.create(dataIn, tracker, logVersion);
      reader.setMaxOpSize(maxOpSize);
      state = State.OPEN;
    } finally {
      if (state != State.OPEN) {
        state = State.CLOSED;
      }
    }
  }

  // -----------------------------------------------------------------------
  // EditLogInputStream implementation
  // -----------------------------------------------------------------------

  @Override
  public String getName() {
    return name;
  }

  @Override
  public long getFirstTxId() {
    return firstTxId;
  }

  @Override
  public long getLastTxId() {
    return lastTxId;
  }

  @Override
  public void close() throws IOException {
    state = State.CLOSED;
    reader = null;
    tracker = null;
  }

  @Override
  protected FSEditLogOp nextOp() throws IOException {
    if (state == State.UNINIT) {
      try {
        init();
      } catch (EditLogFileInputStream.LogHeaderCorruptException e) {
        throw new IOException("Corrupt header in segment " + name, e);
      }
    }
    if (state == State.CLOSED) {
      return null;
    }
    return reader.readOp(false);
  }

  @Override
  protected FSEditLogOp nextValidOp() {
    try {
      return reader == null ? null : reader.readOp(true);
    } catch (Exception e) {
      return null;
    }
  }

  @Override
  public int getVersion(boolean verifyVersion) throws IOException {
    if (state == State.UNINIT) {
      try {
        init();
      } catch (EditLogFileInputStream.LogHeaderCorruptException e) {
        throw new IOException("Corrupt header in segment " + name, e);
      }
    }
    return logVersion;
  }

  @Override
  public long getPosition() {
    return tracker == null ? 0 : tracker.getPos();
  }

  @Override
  public long length() throws IOException {
    return data.length;
  }

  @Override
  public boolean isInProgress() {
    return inProgress;
  }

  @Override
  public void setMaxOpSize(int maxOpSize) {
    this.maxOpSize = maxOpSize;
    if (reader != null) {
      reader.setMaxOpSize(maxOpSize);
    }
  }

  @Override
  public boolean isLocalLog() {
    // Data is already in memory – treat as equivalent to local.
    return true;
  }

  @Override
  public String toString() {
    return name;
  }
}
