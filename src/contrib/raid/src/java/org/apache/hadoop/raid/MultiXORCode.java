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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class MultiXORCode implements ErasureCode {
  public static final Log LOG = LogFactory.getLog(
                                  "org.apache.hadoop.raid.ReedSolomonDecoder");

  private final int stripeSize;
  private final int paritySize;

  public MultiXORCode(int stripeSize, int paritySize) {
    this.paritySize = paritySize;
    this.stripeSize = stripeSize;
    assert(paritySize >= stripeSize / 2);
  }

  @Override
  public void encode(int[] message, int[] parity) {
    assert(message.length == stripeSize && parity.length == paritySize);

    int cols = stripeSize / 2; 
    // clear the additional parities first
    parity[cols] = parity[cols + 1] = 0;
    // init the code values
    for (int i = 0; i < cols; i++)
    {
      parity[i] = message[i];
      parity[cols] ^= message[i];
    }
    // xor the rest properly
    for (int i = cols; i < stripeSize; i++)
    {
      parity[i % cols] ^= message[i];
      parity[cols + 1] ^= message[i];
    }
  }

  @Override
  // TODO: I need to recode this following the same principle behind ReedSolomonCode.java
  // this means that first try to recover using column parity, if its not possible then
  // use full row parities (check if neighbors data is not 0, then is heavy decoder)
  public void decode(int[] data, int[] erasedLocation, int[] erasedValue) {
    assert(erasedLocation.length == erasedValue.length);)

    // we need to see if the erasures are actually recoverable... checking the data and
    // parities is possible to see what kind of erasure we are trying to fix
    for (int i = 0; i < erasedLocation.length; i++)
    {
      boolean usingHeavyDecoder = false;
      // lets try if this position contains data...
      try
      {
        usingHeavyDecoder = data[erasedLocation[i] - 1] != 0 ? true : false;
      }
      catch(IndexOutOfBoundsException ex)
      {
        // if not, lets see the next one...
        try
        {
          usingHeavyDecoder = data[erasedLocation[i] + 1] != 0 ? true : false;
        }
        catch(IndexOutOfBoundsException ey)
        {
          throw new UnsupportedOperationException("MXOR decoder cannot recover this error");
        }
      }

      if (usingHeavyDecoder)
        applyHeavyRules(data, erasedLocation, erasedValue);
      else
        applyLightRules(data, erasedLocation, erasedValue);
    } 
  }

  /**
   * This method will recover any stand-alone erasure without problems, using
   * the basic principles behind MXOR.
   *
   * @param data            Input with all the data retrieved from the DFS
   * @param erasedLocation  Array with all the erased locations inside the DFS
   * @param erasedValue     Output array with all the recovered data values
   */
  private void applyLightRules(int[] data, int[] erasedLocation, int[] erasedValue)
  {
    // for each erased location lets decode using the simple parity approach
    int cols = stripeSize / 2;
    for (int i = 0; i < erasedLocation.length; i++) {
      // the output is the xor between the parity and the surviving data
      int erasedPos = erasedLocation[i] - paritySize;
      erasedValue[i] = data[erasedPos % cols];
      if (erasedPos < cols)
        erasedValue[i] ^= data[erasedLocation[i] + cols];
      else
        erasedValue[i] ^= data[erasedLocation[i] - cols];
    }
  }

  /**
   * This method will recover any column erasure without problems, using
   * the basic principles behind MXOR.
   *
   * @param data            Input with all the data retrieved from the DFS
   * @param erasedLocation  Array with all the erased locations inside the DFS
   * @param erasedValue     Output array with all the recovered data values
   */
  private void applyHeavyRules(int[] data, int[] erasedLocation, int[] erasedValue)
  {
    int cols = stripeSize / 2;
    for (int i = 0; i < erasedLocation.length; i++) {
      // the output is the xor between the parity and the surviving data
      int erasedPos = erasedLocation[i] - paritySize;
      if (erasedPos < cols)
      {
        erasedValue[i] = data[cols]; // XOR'ed with upper row parity
        // the data will be the XORing of all the survival row...
        for (int j = 0; j < cols; j++)
          erasedValue[i] ^= data[paritySize + j];
      }
      else
      {
        erasedValue[i] = data[cols + 1]; // XOR'ed with lower row parity
        // the data will be the XORing of all the survival row...
        for (int j = 0; j < cols; j++)
          erasedValue[i] ^= data[paritySize + cols + j];
      }
    }
    //LOG.debug("(MXOR) erasedLocation is " + convertArrayToString(erasedLocation));
    //throw new UnsupportedOperationException("MXOR decoder cannot recover this error");
  }
  
  /**
   * This function calculates the necessary blocks to perform the decoding
   * (Keep isolated from RS Decoder for any new code).
   * 
   * @param erasedLocation    Input data with all the erased positions
   * @param locationsToFetch  Output data with the blocks required
   * @param doLightDecode     Handles the different decoding processes
   */
  public void blocksToFetch(int[] erasedLocation, int[] locationsToFetch, boolean doLightDecode) {
    LOG.info("(MXOR) blocksToFetch: doLightDecode is " + doLightDecode);
    LOG.info("(MXOR) erasedLocation " + convertArrayToString(erasedLocation));
	
    // we need to identify the need of blocks accordingly to the erasures
    int cols = stripeSize / 2;
    // we don't need to retrieve all the blocks..
    for (int i = 0; i < locationsToFetch.length; i++)
      locationsToFetch[i] = 0;
    // now, we retrieve the column parity
    int erasedPos = erasedLocation[0] - paritySize;
    locationsToFetch[erasedPos % cols] = 1; // corresponding parity...
    // after that, lets bring the blocks!!
    if (doLightDecode)
    {
      LOG.debug("(MXOR) Executing light decode...");
      // we rescue the companion data
      if (erasedPos < cols) {
        // it's in the first half of the data
        locationsToFetch[erasedLocation[0] + cols] = 1; // the other data block... 
      } else {
        // it's in the second half of the data
        locationsToFetch[erasedLocation[0] - cols] = 1; // the other data block...
      }  
    }
    else
    {
      LOG.debug("(MXOR) Executing heavy decode...");
      // here, we need to recover all the data linked to the erasures
      if (erasedPos < cols) {
        // now, retrieve all the elements in the same row...
        for (int i = paritySize; i < paritySize + cols; i++)
        {
          if (i != erasedLocation[0])
            locationsToFetch[i] = 1;
        }
        // at last, rescue the row parity
        locationsToFetch[cols] = 1;
      } else {
        // now, retrieve all the elements in the same row...
        for (int i = paritySize + cols; i < stripeSize; i++)
        {
          if (i != erasedLocation[0])
            locationsToFetch[i] = 1;
        }
        // at last, rescue the row parity
        locationsToFetch[cols + 1] = 1;
      }
    }
  }

  public static String convertArrayToString(int[] array) {
    String str =""+array[0];
    for(int i=1;i<array.length;i++)
      str = str+", "+array[i];

    return str;
  }

  @Override
  public int stripeSize() {
    return this.stripeSize;
  }

  @Override
  public int paritySize() {
    return this.paritySize;
  }

  @Override
  public int symbolSize() {
    return (int) 256; // 1 byte
  }
}
