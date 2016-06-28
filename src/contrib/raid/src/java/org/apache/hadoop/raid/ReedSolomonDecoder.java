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
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockMissingException;
import org.apache.hadoop.fs.ChecksumException;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Progressable;

public class ReedSolomonDecoder extends Decoder {
  public static final Log LOG = LogFactory.getLog(
                                  "org.apache.hadoop.raid.ReedSolomonDecoder");
  private final ErasureCode[] reedSolomonCode;
  private long decodeTime;
  private long waitTime;
  ExecutorService parallelDecoder;
  Semaphore decodeOps;
  // these two lines should be changed to support more codes
  private final ErasureCode[] mxorCode;
  private final boolean useMXOR;

  public ReedSolomonDecoder(
    Configuration conf, int stripeSize, int paritySizeRS, int paritySizeSRC) {
    super(conf, stripeSize, paritySizeRS, paritySizeSRC);
    this.paritySize = paritySizeRS + paritySizeSRC; //just in case
    this.reedSolomonCode = new ReedSolomonCode[parallelism];
    // we instantiate the mxorcode objects as well
    this.mxorCode = new MultiXORCode[parallelism];
    for (int i = 0; i < parallelism; i++) {
      reedSolomonCode[i] =
        new ReedSolomonCode(stripeSize, paritySizeRS, paritySizeSRC);
      mxorCode[i] = new MultiXORCode(stripeSize, paritySizeRS);
    }
    decodeOps = new Semaphore(parallelism);
    this.useMXOR = conf.getBoolean("raid.codes.useMXOR", false);
    LOG.info("Initialized ReedSolomonDecoder" +
    		" with paritySizeSRC = "+paritySizeSRC+"using MXOR = "+useMXOR);
  }

  @Override
  protected void fixErasedBlockImpl(
      FileSystem fs, Path srcFile,
      FileSystem parityFs, Path parityFile,
       long blockSize, long errorOffset, long limit,
       OutputStream out, Progressable reporter, boolean doLightDecode) throws IOException {
    FSDataInputStream[] inputs = new FSDataInputStream[stripeSize + paritySize];
    int[] erasedLocations = buildInputs(fs, srcFile, parityFs, parityFile,
                                        errorOffset, inputs, doLightDecode);
    int blockIdxInStripe = ((int)(errorOffset/blockSize)) % stripeSize;
    int erasedLocationToFix = paritySize + blockIdxInStripe;

    // Allows network reads to go on while decode is going on.
    int boundedBufferCapacity = 2;
    parallelDecoder = Executors.newFixedThreadPool(parallelism);
    ParallelStreamReader parallelReader = new ParallelStreamReader(
      reporter, inputs, bufSize, parallelism, boundedBufferCapacity, blockSize);
    parallelReader.start();
    decodeTime = 0;
    waitTime = 0;
    try {
      writeFixedBlock(inputs, erasedLocations, erasedLocationToFix,
                      limit, out, reporter, parallelReader, doLightDecode);
    } finally {
      // Inputs will be closed by parallelReader.shutdown().
      parallelReader.shutdown();
      LOG.info("Time spent in read " + parallelReader.readTime +
        ", decode " + decodeTime + " wait " + waitTime);
      parallelDecoder.shutdownNow();
    }
  }

  protected int[] buildInputs(FileSystem fs, Path srcFile,
                              FileSystem parityFs, Path parityFile,
                              long errorOffset, FSDataInputStream[] inputs,
                              boolean doLightDecode)
      throws IOException {
    LOG.info("Building inputs to recover block starting at " + errorOffset);
    try {
      FileStatus srcStat = fs.getFileStatus(srcFile);
      long blockSize = srcStat.getBlockSize();
      long blockIdx = (int)(errorOffset / blockSize);
      long stripeIdx = blockIdx / stripeSize;
      LOG.info("FileSize = " + srcStat.getLen() + ", blockSize = " + blockSize +
               ", blockIdx = " + blockIdx + ", stripeIdx = " + stripeIdx);
      ArrayList<Integer> erasedLocations = new ArrayList<Integer>();
      //ArrayList<Integer> erasedLocationsLight = new ArrayList<Integer>();
      int[] locationsToFetch = new int[paritySize+stripeSize];
      for(int i=0;i<paritySize+stripeSize;i++) {
    	  locationsToFetch[i] = 0;
      }

  	  for (int i = paritySize; i < paritySize + stripeSize; i++) {
  		  long offset = blockSize * (stripeIdx * stripeSize + i - paritySize);
  		  if (offset == errorOffset) {
	          LOG.info(srcFile + ":" + offset +
	              " is known to have error, adding zeros as input " + i);
	          inputs[i] = new FSDataInputStream(new RaidUtils.ZeroInputStream(
	              offset + blockSize));
	          erasedLocations.add(i);
  		  }
  	  }
  	  int[] erasedLocationsArray = new int[erasedLocations.size()];
  	  for(int i = 0; i < erasedLocations.size(); i++) {
  		  erasedLocationsArray[i] = erasedLocations.get(i);
  	  }
  	  // the way how we select the blocks to fecth must be code-dependent
      LOG.info("Using MXOR: " + useMXOR);
  	  if (!useMXOR)
  	    blocksToFetch(erasedLocationsArray, locationsToFetch, doLightDecode);
  	  else
        ((MultiXORCode)mxorCode[0]).blocksToFetch(erasedLocationsArray, 
          locationsToFetch, doLightDecode);
  	  LOG.info("locationsToFetch "+convertArrayToString(locationsToFetch));
  	  for(int i = 0; i < locationsToFetch.length; i++) {
  		  FSDataInputStream in;
  		  if(i<paritySize) {
  			  long offset = blockSize * (stripeIdx * paritySize + i);
  			  if(locationsToFetch[i]>0) {
    			  in = parityFs.open(
    			          parityFile, conf.getInt("io.file.buffer.size", 64 * 1024));
    			  LOG.info("Adding " + parityFile + ":" + offset + " as input " + i);
    			  in.seek(offset);
        		  inputs[i] = in;
  			  }
  			  else {
  				  inputs[i] = new FSDataInputStream(new RaidUtils.ZeroInputStream(
  			              offset + blockSize));

  				  LOG.info("Adding zeros as input "+i+" for offset "+offset);
  			  }
  		  }
  		  else {
  			  long offset = blockSize * (stripeIdx * stripeSize + i - paritySize);
  			  if(locationsToFetch[i]>0) {
    			  in = fs.open(
    			            srcFile, conf.getInt("io.file.buffer.size", 64 * 1024));
    			  LOG.info("Adding " + srcFile + ":" + offset + " as input " + i);
    			  in.seek(offset);
        		inputs[i] = in;
  			  }
  			  else {
  				  inputs[i] = new FSDataInputStream(new RaidUtils.ZeroInputStream(
  			              offset + blockSize));

  				  LOG.info("Adding zeros as input "+i+" for offset "+offset);
  			  }
  		  }
  	  }

      // Error overflow!!
      if (!useMXOR && erasedLocations.size() > paritySize) {
        String msg = "Too many erased locations: " + erasedLocations.size();
        LOG.error(msg);
        throw new IOException(msg);
      }
      if (useMXOR && erasedLocations.size() > 2) {
        String msg = "Too many erased locations: " + erasedLocations.size();
        LOG.error(msg);
        throw new IOException(msg);
      }
      int[] locs = new int[erasedLocations.size()];
      for (int i = 0; i < locs.length; i++) {
        locs[i] = erasedLocations.get(i);
      }
      return locs;
    } catch (IOException e) {
      RaidUtils.closeStreams(inputs);
      throw e;
    }

  }

  public static String convertArrayToString(int[] array) {
	  String str =""+array[0];
	  for(int i=1;i<array.length;i++)
		  str = str+", "+array[i];

	  return str;
  }
  /**
   * Decode the inputs provided and write to the output.
   * @param inputs array of inputs.
   * @param erasedLocations indexes in the inputs which are known to be erased.
   * @param erasedLocationToFix index in the inputs which needs to be fixed.
   * @param limit maximum number of bytes to be written.
   * @param out the output.
   * @throws IOException
   */
  void writeFixedBlock(
          FSDataInputStream[] inputs,
          int[] erasedLocations,
          int erasedLocationToFix,
          long limit,
          OutputStream out,
          Progressable reporter,
          ParallelStreamReader parallelReader,
          boolean doLightDecode) throws IOException {

    LOG.info("Need to write " + limit +
             " bytes for erased location index " + erasedLocationToFix);
    int[] tmp = new int[inputs.length];
    int[] decoded = new int[erasedLocations.length];
    // Loop while the number of written bytes is less than the max.
    for (long written = 0; written < limit; ) {
      erasedLocations = readFromInputs(
        inputs, erasedLocations, limit, reporter, parallelReader, doLightDecode);
      if (decoded.length != erasedLocations.length) {
        decoded = new int[erasedLocations.length];
      }
      LOG.info("in writeFixedBlock, " +
      		"erasedLocations after readFromInputs = "+convertArrayToString(erasedLocations));
      int toWrite = (int)Math.min(bufSize, limit - written);

      int partSize = (int) Math.ceil(bufSize * 1.0 / parallelism);
      try {
        long startTime = System.currentTimeMillis();
        for (int i = 0; i < parallelism; i++) {
          decodeOps.acquire(1);
          int start = i * partSize;
          int count = Math.min(bufSize - start, partSize);
          // here, the mxor code is actually assigned...
          if (!useMXOR)
            parallelDecoder.execute(new DecodeOp(
              readBufs, writeBufs, start, count,
              erasedLocations, reedSolomonCode[i]));
          else
            parallelDecoder.execute(new DecodeOp(
              readBufs, writeBufs, start, count,
              erasedLocations, mxorCode[i]));
        }
        decodeOps.acquire(parallelism);
        decodeOps.release(parallelism);
        decodeTime += (System.currentTimeMillis() - startTime);
      } catch (InterruptedException e) {
        throw new IOException("Interrupted while waiting for read result");
      }


      for (int i = 0; i < erasedLocations.length; i++) {
        if (erasedLocations[i] == erasedLocationToFix) {
          out.write(writeBufs[i], 0, toWrite);
          written += toWrite;
          break;
        }
      }
    }
  }

  int[] readFromInputs(
          FSDataInputStream[] inputs,
          int[] erasedLocations,
          long limit,
          Progressable reporter,
          ParallelStreamReader parallelReader,
          boolean doLightDecode) throws IOException {
    ParallelStreamReader.ReadResult readResult;
    try {
      long start = System.currentTimeMillis();
      readResult = parallelReader.getReadResult();
      waitTime += (System.currentTimeMillis() - start);
    } catch (InterruptedException e) {
      throw new IOException("Interrupted while waiting for read result");
    }

    // Process io errors, we can tolerate upto paritySize errors.
    for (int i = 0; i < readResult.ioExceptions.length; i++) {
      IOException e = readResult.ioExceptions[i];
      if (e == null) {
        continue;
      }
      if (e instanceof BlockMissingException) {
        LOG.warn("Encountered BlockMissingException in stream " + i);
      } else if (e instanceof ChecksumException) {
        LOG.warn("Encountered ChecksumException in stream " + i);
      } else {
        throw e;
      }

      // Found a new erased location.
      //TODO: Shouldn't it be >
      if (erasedLocations.length == paritySize) {
        String msg = "Too many read errors";
        LOG.error(msg);
        throw new IOException(msg);
      }

      // Add this stream to the set of erased locations.
      int[] newErasedLocations = new int[erasedLocations.length + 1];
      for (int j = 0; j < erasedLocations.length; j++) {
        newErasedLocations[j] = erasedLocations[j];
      }
      newErasedLocations[newErasedLocations.length - 1] = i;
      erasedLocations = newErasedLocations;
      LOG.info("in readFromInputs, erasedLocations is now "+convertArrayToString(erasedLocations));
    }
    readBufs = readResult.readBufs;

    // If there are more than one erasedLocations, let the heavy decoder take care of it.
    if(doLightDecode&&(erasedLocations.length>1))
    	throw new IOException("LIGHT DECODER FAILED");

    return erasedLocations;
  }

  class DecodeOp implements Runnable {
    byte[][] readBufs;
    byte[][] writeBufs;
    int startIdx;
    int count;
    int[] erasedLocations;
    int[] tmpInput;
    int[] tmpOutput;
    ErasureCode rs;
    DecodeOp(byte[][] readBufs, byte[][] writeBufs,
             int startIdx, int count, int[] erasedLocations,
             ErasureCode rs) {
      this.readBufs = readBufs;
      this.writeBufs = writeBufs;
      this.startIdx = startIdx;
      this.count = count;
      this.erasedLocations = erasedLocations;
      this.tmpInput = new int[readBufs.length];
      this.tmpOutput = new int[erasedLocations.length];
      this.rs = rs;
    }

    public void run() {
      try {
        performDecode();
      } finally {
        decodeOps.release();
      }
    }

    private void performDecode() {
      for (int idx = startIdx; idx < startIdx + count; idx++) {
        for (int i = 0; i < tmpOutput.length; i++) {
          tmpOutput[i] = 0;
        }
        for (int i = 0; i < tmpInput.length; i++) {
          tmpInput[i] = readBufs[i][idx] & 0x000000FF;
        }
        // the decoder is the same (given as a parameter)
        rs.decode(tmpInput, erasedLocations, tmpOutput);
        for (int i = 0; i < tmpOutput.length; i++) {
          writeBufs[i][idx] = (byte)tmpOutput[i];
        }
      }
    }
  }

  public void blocksToFetch(int[] erasedLocation, int[] locationsToFetch,
      boolean doLightDecode) {
    int paritySizeRS = paritySize - paritySizeSRC;
    int simpleParityDegree = -1;
    //TODO: fix the below about simpleParityDegree
    if(paritySizeSRC>0)
      simpleParityDegree = (stripeSize + paritySizeRS)/paritySizeSRC;

    int flagErased = 0;
    int locationsLength = 0;
    double singleErasureGroup;
    LOG.info("blocksToFetch: doLightDecode is "+doLightDecode);
    LOG.info("erasedLocation "+convertArrayToString(erasedLocation));

    // The following lines perform OK for normal RS decoding
    if((!doLightDecode)||(paritySizeSRC==0)) {
      // recover all the parities
      for (int i = 0; i < paritySizeSRC + paritySizeRS + stripeSize; i++) {
        locationsToFetch[i] = 1;
      }
      // then, all the surviving data
      for (int i = 0; i < erasedLocation.length; i++) {
        locationsToFetch[erasedLocation[i]] = 0;
      }
      return;
    }

    if (erasedLocation.length == 0) {
      return;
    }
    // Initialize the locations to fetch to
    for (int i = 0; i < paritySizeSRC + paritySizeRS + stripeSize; i++) {
      locationsToFetch[i] = 0;
    }
    // First check if the is a single failure
    if (erasedLocation.length == 1) {
      // Find the simpleXOR group that the erased block is a member of
      if (erasedLocation[0] >= paritySizeSRC) {
        singleErasureGroup = Math.ceil(
                            ((erasedLocation[0] - paritySizeSRC + 1)) /
                            ((float)simpleParityDegree));
       }
      else{
        singleErasureGroup = erasedLocation[0] + 1;
      }
      // Indicate the blocks that need to be communicated
      for (int f = 0; f < simpleParityDegree; f++) {
        // parityRS and stripe blocks
        locationsToFetch[paritySizeSRC +
                         ((int)singleErasureGroup - 1)
                         * simpleParityDegree + f] = 1;
      }
      locationsToFetch[(int)singleErasureGroup - 1] = 1; 		//SimpleXOR block
  		for (int i = 0; i<paritySizeSRC + paritySizeRS + stripeSize; i++){
  			if (i == erasedLocation[0]) {
  				locationsToFetch[i]=0;
  			}
  		}
    }
  	else if (erasedLocation.length > 1){
  	  for (int i = 0; i < stripeSize + paritySizeRS; i++) {
  	    for (int j = 0; j < erasedLocation.length; j++){
  	      if(erasedLocation[j] == paritySizeSRC + i){
  					flagErased = 1;
  				}
  			}
  			if (flagErased == 0) {
  				locationsToFetch[paritySizeSRC + i] = 1;
  				locationsLength++;
  				if (locationsLength == stripeSize)
  					return;
  			}
  			else {
  				flagErased = 0;
  			}
  		}
  	}
  }
}
