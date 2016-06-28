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

package org.apache.hadoop.raid;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Progressable;

public class ReedSolomonEncoder extends Encoder {
  public static final Log LOG = LogFactory.getLog(
                                  "org.apache.hadoop.raid.ReedSolomonEncoder");
  private final ErasureCode reedSolomonCode;
  // these two lines must be changed to support other codes...
  private final ErasureCode mxorCode;
  private final boolean useMXOR;

  public ReedSolomonEncoder(
    Configuration conf, int stripeSize, int paritySizeRS, int paritySizeSRC) {
    super(conf, stripeSize, paritySizeRS, paritySizeSRC);
    this.paritySize = paritySizeRS + paritySizeSRC; // just in case
    this.reedSolomonCode =
      new ReedSolomonCode(stripeSize, paritySizeRS, paritySizeSRC);
    // here, we ignore the local parities used by Xorbas LRC
    this.mxorCode = new MultiXORCode(stripeSize, paritySizeRS);
    this.useMXOR = conf.getBoolean("raid.codes.useMXOR", false);
    LOG.info("Initialized ReedSolomonEncoder" +
    		" with paritySizeSRC = "+paritySizeSRC + " using MXOR = " + useMXOR);
  }

  @Override
  protected void encodeStripeImpl(
    InputStream[] blocks,
    long stripeStartOffset,
    long blockSize,
    OutputStream[] outs,
    Progressable reporter) throws IOException {
    int boundedBufferCapacity = 1;
    ParallelStreamReader parallelReader = new ParallelStreamReader(
      reporter, blocks, bufSize, parallelism, boundedBufferCapacity, blockSize);
    parallelReader.start();
     try {
       encodeStripeParallel(
         blocks, stripeStartOffset, blockSize, outs, reporter, parallelReader);
     } finally {
       parallelReader.shutdown();
     }
   }

   private void encodeStripeParallel(
     InputStream[] blocks,
     long stripeStartOffset,
     long blockSize,
     OutputStream[] outs,
     Progressable reporter,
     ParallelStreamReader parallelReader) throws IOException {

    int[] data = new int[stripeSize];
    int[] code = new int[paritySize];

    for (long encoded = 0; encoded < blockSize; encoded += bufSize) {
      // Read some data from each block = bufSize.
      ParallelStreamReader.ReadResult readResult;
      try {
        readResult = parallelReader.getReadResult();
      } catch (InterruptedException e) {
        throw new IOException("Interrupted while waiting for read result");
      }
      // Cannot tolerate any IO errors.
      IOException readEx = readResult.getException();
      if (readEx != null) {
        throw readEx;
      }

      // Encode the data read.
      for (int j = 0; j < bufSize; j++) {
        performEncode(readResult.readBufs, writeBufs, j, data, code);
      }
      reporter.progress();

      // Now that we have some data to write, send it to the temp files.
      for (int i = 0; i < paritySize; i++) {
        outs[i].write(writeBufs[i], 0, bufSize);
        reporter.progress();
      }
    }
  }

  void performEncode(byte[][] readBufs, byte[][] writeBufs, int idx,
                          int[] data, int[] code) {
    for (int i = 0; i < paritySize; i++) {
      code[i] = 0;
    }
    for (int i = 0; i < stripeSize; i++) {
      data[i] = readBufs[i][idx] & 0x000000FF;
    }
    // here, some kind of factory could be a good idea...
    if (!useMXOR)
      reedSolomonCode.encode(data, code);
    else
      mxorCode.encode(data, code);
    for (int i = 0; i < paritySize; i++) {
      writeBufs[i][idx] = (byte)code[i];
    }
  }

  @Override
  public Path getParityTempPath() {
    return new Path(RaidNode.rsTempPrefix(conf));
  }

}
