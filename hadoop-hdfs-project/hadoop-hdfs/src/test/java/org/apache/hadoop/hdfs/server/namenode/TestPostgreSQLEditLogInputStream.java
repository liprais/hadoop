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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Unit tests for {@link PostgreSQLEditLogInputStream}.
 *
 * <p>These tests validate that bytes written by the standard
 * {@link EditLogFileOutputStream} can be round-tripped through
 * {@link PostgreSQLEditLogInputStream} – i.e. that the PGSQL stream correctly
 * parses the same binary edit-log format that the file-based stream uses.
 * No live database connection is required.
 */
public class TestPostgreSQLEditLogInputStream {

  static {
    EditLogFileOutputStream.setShouldSkipFsyncForTesting(true);
  }

  // -----------------------------------------------------------------------
  // Helpers
  // -----------------------------------------------------------------------

  /**
   * Writes two {@code OP_MKDIR} operations to a temp file using
   * {@link EditLogFileOutputStream}, reads the raw bytes, and returns them.
   */
  private byte[] buildEditLogSegmentBytes(long firstTxId) throws IOException {
    Configuration conf = new Configuration();
    File tmp = new File(
        GenericTestUtils.getTempPath("testPgsqlEditLog-" + firstTxId));
    tmp.deleteOnExit();

    EditLogFileOutputStream elos =
        new EditLogFileOutputStream(conf, tmp, 4096);
    elos.create(NameNodeLayoutVersion.CURRENT_LAYOUT_VERSION);

    FSEditLogOp.OpInstanceCache cache = new FSEditLogOp.OpInstanceCache();
    PermissionStatus perms = PermissionStatus.createImmutable(
        "user1", "group1", FsPermission.createImmutable((short) 0755));

    FSEditLogOp.MkdirOp op1 = FSEditLogOp.MkdirOp.getInstance(cache);
    op1.reset();
    op1.setRpcCallId(1);
    op1.setTransactionId(firstTxId);
    op1.setInodeId(100L);
    op1.setPath("/testdir1");
    op1.setPermissionStatus(perms);
    elos.write(op1);

    FSEditLogOp.MkdirOp op2 = FSEditLogOp.MkdirOp.getInstance(cache);
    op2.reset();
    op2.setRpcCallId(2);
    op2.setTransactionId(firstTxId + 1);
    op2.setInodeId(101L);
    op2.setPath("/testdir2");
    op2.setPermissionStatus(perms);
    elos.write(op2);

    elos.setReadyToFlush();
    elos.flushAndSync(false);
    elos.close();

    // Read the file bytes.
    byte[] bytes;
    try (FileInputStream fis = new FileInputStream(tmp)) {
      bytes = fis.readAllBytes();
    }
    tmp.delete();
    return bytes;
  }

  // -----------------------------------------------------------------------
  // Tests
  // -----------------------------------------------------------------------

  /**
   * Verifies that two ops written by {@link EditLogFileOutputStream} can be
   * read back by {@link PostgreSQLEditLogInputStream}.
   */
  @Test
  @Timeout(value = 30)
  public void testReadOpsFromBytes() throws Exception {
    long firstTxId = 1L;
    byte[] bytes = buildEditLogSegmentBytes(firstTxId);

    PostgreSQLEditLogInputStream stream = new PostgreSQLEditLogInputStream(
        "test-segment", bytes, firstTxId, firstTxId + 1, false);

    // The layout version should match the version written to the header.
    assertEquals(NameNodeLayoutVersion.CURRENT_LAYOUT_VERSION,
        stream.getVersion(true));

    // Read op 1
    FSEditLogOp op1 = stream.readOp();
    assertNotNull(op1);
    assertEquals(firstTxId, op1.getTransactionId());
    assertEquals(FSEditLogOpCodes.OP_MKDIR, op1.opCode);

    // Read op 2
    FSEditLogOp op2 = stream.readOp();
    assertNotNull(op2);
    assertEquals(firstTxId + 1, op2.getTransactionId());
    assertEquals(FSEditLogOpCodes.OP_MKDIR, op2.opCode);

    // No more ops
    assertNull(stream.readOp());

    stream.close();
  }

  /**
   * Verifies metadata fields (getName, getFirstTxId, getLastTxId, isInProgress,
   * isLocalLog, length).
   */
  @Test
  @Timeout(value = 30)
  public void testMetadata() throws Exception {
    byte[] bytes = buildEditLogSegmentBytes(5L);

    PostgreSQLEditLogInputStream stream = new PostgreSQLEditLogInputStream(
        "segment-5", bytes, 5L, 10L, false);

    assertEquals("segment-5", stream.getName());
    assertEquals(5L, stream.getFirstTxId());
    assertEquals(10L, stream.getLastTxId());
    assertFalse(stream.isInProgress());
    assertTrue(stream.isLocalLog());
    assertEquals(bytes.length, stream.length());
    stream.close();
  }

  /**
   * Verifies that an in-progress stream can be created and read.
   */
  @Test
  @Timeout(value = 30)
  public void testInProgressStream() throws Exception {
    long firstTxId = 100L;
    byte[] bytes = buildEditLogSegmentBytes(firstTxId);

    PostgreSQLEditLogInputStream stream = new PostgreSQLEditLogInputStream(
        "inprogress-100", bytes,
        firstTxId, HdfsServerConstants.INVALID_TXID, true);

    assertTrue(stream.isInProgress());

    // Should still be able to read valid ops.
    FSEditLogOp op = stream.readOp();
    assertNotNull(op);
    assertEquals(firstTxId, op.getTransactionId());

    stream.close();
  }

  /**
   * Verifies that {@link PostgreSQLEditLogInputStream#skipUntil} skips to the
   * correct op.
   */
  @Test
  @Timeout(value = 30)
  public void testSkipUntil() throws Exception {
    long firstTxId = 1L;
    byte[] bytes = buildEditLogSegmentBytes(firstTxId);

    PostgreSQLEditLogInputStream stream = new PostgreSQLEditLogInputStream(
        "test-skip", bytes, firstTxId, firstTxId + 1, false);

    // Skip to second transaction.
    boolean found = stream.skipUntil(firstTxId + 1);
    assertTrue(found);

    FSEditLogOp op = stream.readOp();
    assertNotNull(op);
    assertEquals(firstTxId + 1, op.getTransactionId());

    stream.close();
  }

  /**
   * Verifies that an empty byte array causes a
   * {@link EditLogFileInputStream.LogHeaderCorruptException} to be wrapped in
   * an {@link IOException}.
   */
  @Test
  @Timeout(value = 30)
  public void testEmptyBytesThrows() {
    PostgreSQLEditLogInputStream stream = new PostgreSQLEditLogInputStream(
        "empty-segment", new byte[0], 1L, -1L, true);
    assertThrows(IOException.class,
        () -> stream.getVersion(true));
  }
}
