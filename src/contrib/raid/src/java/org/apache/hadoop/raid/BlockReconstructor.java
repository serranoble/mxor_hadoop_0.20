package org.apache.hadoop.raid;

import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.channels.SocketChannel;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DataTransferProtocol;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hdfs.protocol.FSConstants.DatanodeReportType;
import org.apache.hadoop.hdfs.server.common.HdfsConstants;
import org.apache.hadoop.hdfs.server.datanode.BlockSender;
import org.apache.hadoop.hdfs.server.datanode.FSDataset;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.util.DataChecksum;
import org.apache.hadoop.util.Progressable;

/**
 * this class implements the actual reconstructing functionality
 * we keep this in a separate class so that
 * the distributed block fixer can use it
 */
abstract class BlockReconstructor extends Configured {

  public static final Log LOG = LogFactory.getLog(BlockReconstructor.class);

  private String xorPrefix;
  private String rsPrefix;
  private final XOREncoder xorEncoder;
  private final XORDecoder xorDecoder;
  //private final ReedSolomonEncoder rsEncoder;
  //private final ReedSolomonDecoder rsDecoder;
  private static int dataTransferProtocolVersion = -1;

  BlockReconstructor(Configuration conf) throws IOException {
    super(conf);

    xorPrefix = RaidNode.xorDestinationPath(getConf()).toUri().getPath();
    if (!xorPrefix.endsWith(Path.SEPARATOR)) {
      xorPrefix += Path.SEPARATOR;
    }
    rsPrefix = RaidNode.rsDestinationPath(getConf()).toUri().getPath();
    if (!rsPrefix.endsWith(Path.SEPARATOR)) {
      rsPrefix += Path.SEPARATOR;
    }
    int stripeLength = RaidNode.getStripeLength(getConf());
    xorEncoder = new XOREncoder(getConf(), stripeLength);
    xorDecoder = new XORDecoder(getConf(), stripeLength);
    /* int paritySizeRS = RaidNode.rsParityLength(getConf());
    int paritySizeSRC = RaidNode.paritySizeSRC(getConf());
    rsEncoder = new ReedSolomonEncoder(getConf(), stripeLength,
    		paritySizeRS, paritySizeSRC);
    rsDecoder = new ReedSolomonDecoder(getConf(), stripeLength,
    		paritySizeRS, paritySizeSRC); */

  }

  private static synchronized int getDataTransferProtocolVersion(
      Configuration conf) throws IOException {
    if (dataTransferProtocolVersion == -1) {
      dataTransferProtocolVersion = RaidUtils.getDataTransferProtocolVersion(conf);
    }
    return dataTransferProtocolVersion;
  }

  private boolean isHarFile(String path) {
    return path.lastIndexOf(RaidNode.HAR_SUFFIX) != -1;
  }

  /**
   * Get path for the corresponding source file for a valid parity
   * file. Returns null if it does not exists
   * @param parity the toUri path of the parity file
   * @return the toUri path of the source file
   */
  String getSourceFile(String parity, String prefix) throws IOException {
    if (isHarFile(parity)) {
      return null;
    }
    // remove the prefix
    String src = "/"+parity.substring(prefix.length());

    return src;
  }

  /**
   * checks whether file is xor parity file
   */
  boolean isXorParityFile(Path p) {
    String pathStr = p.toUri().getPath();
    return isXorParityFile(pathStr);
  }

  boolean isXorParityFile(String pathStr) {
    if (pathStr.contains(RaidNode.HAR_SUFFIX)) {
      return false;
    }
    return pathStr.startsWith(xorPrefix);
  }

  int getNumParities(Path p) throws IOException{
    String pathStr = p.toUri().getPath();
    int stripeLength = RaidNode.getStripeLength(getConf());
    DistributedFileSystem parityFs = getDFS(p);
    FileStatus parityStat = parityFs.getFileStatus(p);

    String srcFile = getSourceFile(pathStr, rsPrefix);
    DistributedFileSystem srcFs = getDFS(new Path(srcFile));
    FileStatus srcStat = srcFs.getFileStatus(new Path(srcFile));
    long parityBlockSize = parityStat.getBlockSize();
    long srcBlockSize = srcStat.getBlockSize();
    long parityFileSize = parityStat.getLen();
    long srcFileSize = srcStat.getLen();

    int numParityBlocks = (int) Math.ceil((double)parityFileSize/parityBlockSize);
    int numSrcBlocks = (int) Math.ceil((double)srcFileSize/srcBlockSize);
    int numParities = (int) Math.ceil((double)numParityBlocks*stripeLength/numSrcBlocks);
    LOG.info("isRsParity: parityblockSize = "+parityBlockSize+", " +
    		"sourceblocksize = "+srcBlockSize +
        "parityFileSize = "+parityFileSize+", " +
            "srcFileSize = "+srcFileSize+", numParities = "+numParities);
    return numParities;
  }
  /**
   * checks whether file is rs parity file
   */
  boolean isRsParityFile(Path p) throws IOException{
    String pathStr = p.toUri().getPath();
    if(isRsParityFile(pathStr)) {
      int numParities = getNumParities(p);
      int paritySizeRS = RaidNode.rsParityLength(getConf());
      LOG.info("numParities = "+numParities+", paritySizeRS = "+paritySizeRS);
      if(numParities == paritySizeRS)
        return true;
    }
    return false;
  }

  boolean isRsParityFile(String pathStr) {
    if (pathStr.contains(RaidNode.HAR_SUFFIX)) {
      return false;
    }
    boolean retValue = (pathStr.startsWith(rsPrefix) || pathStr.startsWith("hdfs://localhost:54310"+rsPrefix));
    LOG.info("retValue = "+retValue);
    return retValue;
  }

  /**
   * checks whether file is SRC parity file
   */
  boolean isSRCParityFile(Path p) throws IOException{
    String pathStr = p.toUri().getPath();
    if(isRsParityFile(pathStr)) {
      int numParities = getNumParities(p);
      int paritySizeRS = RaidNode.rsParityLength(getConf());
      int paritySizeSRC = RaidNode.paritySizeSRC(getConf());
      LOG.info("numParities = "+numParities+"" +
      		", paritySizeRS  = "+paritySizeRS+"" +
      				", paritySizeSRC = "+paritySizeSRC);
      if(numParities == paritySizeRS + paritySizeSRC)
        return true;
    }
    return false;
  }



  /**
   * Fix a file, report progress.
   *
   * @return true if file was reconstructed, false if no reconstruction
   * was necessary or possible.
   */
  boolean reconstructFile(Path srcPath, Progressable progress)
      throws IOException {

    if (RaidNode.isParityHarPartFile(srcPath)) {
      return processParityHarPartFile(srcPath, progress);
    }

    // The lost file is a XOR parity file
    if (isXorParityFile(srcPath)) {
      return processParityFile(srcPath, xorEncoder, progress);
    }

    // The following values cannot be global.
    // A file can be either RS encoded or SRC encoded.
    int stripeLength = RaidNode.getStripeLength(getConf());
    int paritySizeRS = RaidNode.rsParityLength(getConf());
    int paritySizeSRC = RaidNode.paritySizeSRC(getConf());

    // The lost file is a ReedSolomon parity file
    if (isRsParityFile(srcPath)) {
      return processParityFile(
          srcPath,
          new ReedSolomonEncoder(getConf(), stripeLength, paritySizeRS, 0),
          progress);
    }

    // The lost file is a Simple Regenerating Code parity file
    if (isSRCParityFile(srcPath)) {
      return processParityFile(
          srcPath,
          new ReedSolomonEncoder(getConf(), stripeLength,
              paritySizeRS, paritySizeSRC),
          progress);
    }

    //ReedSolomonDecoder rsDecoder = new ReedSolomonDecoder(getConf(), stripeLength,
    //    paritySizeRS, paritySizeSRC);
    // The lost file is a source file. It might have a Reed-Solomon parity
    // or XOR parity or both.
    // Look for the Reed-Solomon parity file first. It is possible that the XOR
    // parity file is missing blocks at this point.
    ParityFilePair ppair = ParityFilePair.getParityFile(
        ErasureCodeType.RS, srcPath, getConf());
    Decoder decoder = null;
    if (ppair != null) {
      LOG.info("ppair path = "+ppair.getPath()+"" +
      		", isRS = "+isRsParityFile(ppair.getPath())+"" +
      				", isSRC = "+isSRCParityFile(ppair.getPath()));
      if(isRsParityFile(ppair.getPath()))
        decoder = new ReedSolomonDecoder(getConf(), stripeLength,
            paritySizeRS, 0);
      else
        decoder = new ReedSolomonDecoder(getConf(), stripeLength,
            paritySizeRS, paritySizeSRC);

    } else  {
      ppair = ParityFilePair.getParityFile(
          ErasureCodeType.XOR, srcPath, getConf());
      if (ppair != null) {
        decoder = xorDecoder;
      }
    }

    // If we have a parity file, process the file and reconstruct it.
    if (ppair != null) {
      return processFile(srcPath, ppair, decoder, progress);
    }

    // there was nothing to do
    LOG.warn("Could not find parity file for source file "
        + srcPath + ", ignoring...");
    return false;
  }

  /**
   * Sorts source files ahead of parity files.
   */
  void sortLostFiles(List<String> files) {
    // TODO: We should first fix the files that lose more blocks
    Comparator<String> comp = new Comparator<String>() {
      public int compare(String p1, String p2) {
        if (isXorParityFile(p2) || isRsParityFile(p2)) {
          // If p2 is a parity file, p1 is smaller.
          return -1;
        }
        if (isXorParityFile(p1) || isRsParityFile(p1)) {
          // If p1 is a parity file, p2 is smaller.
          return 1;
        }
        // If both are source files, they are equal.
        return 0;
      }
    };
    Collections.sort(files, comp);
  }

  /**
   * Returns a DistributedFileSystem hosting the path supplied.
   */
  protected DistributedFileSystem getDFS(Path p) throws IOException {
    return (DistributedFileSystem) p.getFileSystem(getConf());
  }

  /**
   * Reads through a source file reconstructing lost blocks on the way.
   * @param srcPath Path identifying the lost file.
   * @throws IOException
   * @return true if file was reconstructed, false if no reconstruction
   * was necessary or possible.
   */
  boolean processFile(Path srcPath, ParityFilePair parityPair,
      Decoder decoder, Progressable progress)
  throws IOException {
    LOG.info("Processing file " + srcPath);

    DistributedFileSystem srcFs = getDFS(srcPath);
    FileStatus srcStat = srcFs.getFileStatus(srcPath);
    long blockSize = srcStat.getBlockSize();
    long srcFileSize = srcStat.getLen();
    String uriPath = srcPath.toUri().getPath();

    int numBlocksReconstructed = 0;
    List<LocatedBlock> lostBlocks = lostBlocksInFile(srcFs, uriPath, srcStat);
    if (lostBlocks.size() == 0) {
      LOG.warn("Couldn't find any lost blocks in file " + srcPath +
          ", ignoring...");
      return false;
    }
    for (LocatedBlock lb: lostBlocks) {
      Block lostBlock = lb.getBlock();
      long lostBlockOffset = lb.getStartOffset();

      LOG.info("Found lost block " + lostBlock +
          ", offset " + lostBlockOffset);

      final long blockContentsSize =
        Math.min(blockSize, srcFileSize - lostBlockOffset);
      File localBlockFile =
        File.createTempFile(lostBlock.getBlockName(), ".tmp");
      localBlockFile.deleteOnExit();
      boolean doLightDecodeOptions[] = {true, false};

      try {
    	  for(boolean doLightDecode:doLightDecodeOptions) {
	    	  try {
	    		  LOG.info("lostBlockOffset = "+lostBlockOffset);
		        decoder.recoverBlockToFile(srcFs, srcPath, parityPair.getFileSystem(),
		            parityPair.getPath(), blockSize,
		            lostBlockOffset, localBlockFile,
		            blockContentsSize, progress, doLightDecode);

		        // Now that we have recovered the file block locally, send it.
		        String datanode = chooseDatanode(lb.getLocations());
		        computeMetadataAndSendReconstructedBlock(datanode, localBlockFile,
		            lostBlock, blockContentsSize);
		        numBlocksReconstructed++;
		        LOG.info("In processFile, block reconstruction successful, numBlocksReconstructed = " +
                ""+numBlocksReconstructed);
		        break; //If the block reconstruction was successful, break out of the loop

		      }catch(IOException e) {
		    	  // Light Decoder failed.
		    	  // So try the Heavy Decoder by setting doLightDecode parameter to false
		    	  // The for loop will set this automatically.

		    	  if(doLightDecode)
		    		  LOG.error("Light Decoder failed. Trying the heavy decoder");
		    	  else
		    		  LOG.error("Unable to decode");
		      }
	      }
      }
      finally {
        localBlockFile.delete();
      }
      progress.progress();
    }


    LOG.info("Reconstructed " + numBlocksReconstructed + " blocks in " + srcPath);
    return true;
  }

  /**
   * Reads through a parity file, reconstructing lost blocks on the way.
   * This function uses the corresponding source file to regenerate parity
   * file blocks.
   * @return true if file was reconstructed, false if no reconstruction
   * was necessary or possible.
   */
  boolean processParityFile(Path parityPath, Encoder encoder,
      Progressable progress)
  throws IOException {
    LOG.info("Processing parity file " + parityPath);
    Path srcPath = sourcePathFromParityPath(parityPath);
    if (srcPath == null) {
      LOG.warn("Could not get regular file corresponding to parity file " +
          parityPath + ", ignoring...");
      return false;
    }

    DistributedFileSystem parityFs = getDFS(parityPath);
    FileStatus parityStat = parityFs.getFileStatus(parityPath);
    long blockSize = parityStat.getBlockSize();
    FileStatus srcStat = getDFS(srcPath).getFileStatus(srcPath);
    long srcFileSize = srcStat.getLen();

    // Check timestamp.
    if (srcStat.getModificationTime() != parityStat.getModificationTime()) {
      LOG.warn("Mismatching timestamp for " + srcPath + " and " + parityPath +
          ", ignoring...");
      return false;
    }

    String uriPath = parityPath.toUri().getPath();
    int numBlocksReconstructed = 0;
    List<LocatedBlock> lostBlocks =
      lostBlocksInFile(parityFs, uriPath, parityStat);
    if (lostBlocks.size() == 0) {
      LOG.warn("Couldn't find any lost blocks in parity file " + parityPath +
          ", ignoring...");
      return false;
    }
    for (LocatedBlock lb: lostBlocks) {
      Block lostBlock = lb.getBlock();
      long lostBlockOffset = lb.getStartOffset();

      LOG.info("Found lost block " + lostBlock +
          ", offset " + lostBlockOffset);

      File localBlockFile =
        File.createTempFile(lostBlock.getBlockName(), ".tmp");
      localBlockFile.deleteOnExit();

      try {
        encoder.recoverParityBlockToFile(parityFs, srcPath, srcFileSize,
            blockSize, parityPath,
            lostBlockOffset, localBlockFile, progress);

        // Now that we have recovered the parity file block locally, send it.
        String datanode = chooseDatanode(lb.getLocations());
        computeMetadataAndSendReconstructedBlock(
            datanode, localBlockFile,
            lostBlock, blockSize);

        numBlocksReconstructed++;
      } finally {
        localBlockFile.delete();
      }
      progress.progress();
    }

    LOG.info("Reconstructed " + numBlocksReconstructed + " blocks in " + parityPath);
    return true;
  }

  /**
   * Reads through a parity HAR part file, reconstructing lost blocks on the way.
   * A HAR block can contain many file blocks, as long as the HAR part file
   * block size is a multiple of the file block size.
   * @return true if file was reconstructed, false if no reconstruction
   * was necessary or possible.
   */
  boolean processParityHarPartFile(Path partFile,
      Progressable progress)
  throws IOException {
    LOG.info("Processing parity HAR file " + partFile);
    // Get some basic information.
    DistributedFileSystem dfs = getDFS(partFile);
    FileStatus partFileStat = dfs.getFileStatus(partFile);
    long partFileBlockSize = partFileStat.getBlockSize();
    LOG.info(partFile + " has block size " + partFileBlockSize);

    // Find the path to the index file.
    // Parity file HARs are only one level deep, so the index files is at the
    // same level as the part file.
    // Parses through the HAR index file.
    HarIndex harIndex = HarIndex.getHarIndex(dfs, partFile);
    String uriPath = partFile.toUri().getPath();
    int numBlocksReconstructed = 0;
    List<LocatedBlock> lostBlocks = lostBlocksInFile(dfs, uriPath,
        partFileStat);
    if (lostBlocks.size() == 0) {
      LOG.warn("Couldn't find any lost blocks in HAR file " + partFile +
          ", ignoring...");
      return false;
    }
    for (LocatedBlock lb: lostBlocks) {
      Block lostBlock = lb.getBlock();
      long lostBlockOffset = lb.getStartOffset();

      File localBlockFile =
        File.createTempFile(lostBlock.getBlockName(), ".tmp");
      localBlockFile.deleteOnExit();

      try {
        processParityHarPartBlock(dfs, partFile, lostBlock,
            lostBlockOffset, partFileStat, harIndex,
            localBlockFile, progress);

        // Now that we have recovered the part file block locally, send it.
        String datanode = chooseDatanode(lb.getLocations());
        computeMetadataAndSendReconstructedBlock(datanode, localBlockFile,
            lostBlock,
            localBlockFile.length());

        numBlocksReconstructed++;
      } finally {
        localBlockFile.delete();
      }
      progress.progress();
    }

    LOG.info("Reconstructed " + numBlocksReconstructed + " blocks in " + partFile);
    return true;
  }

  /**
   * This reconstructs a single part file block by recovering in sequence each
   * parity block in the part file block.
   */
  private void processParityHarPartBlock(FileSystem dfs, Path partFile,
      Block block,
      long blockOffset,
      FileStatus partFileStat,
      HarIndex harIndex,
      File localBlockFile,
      Progressable progress)
  throws IOException {
    String partName = partFile.toUri().getPath(); // Temporarily.
    partName = partName.substring(1 + partName.lastIndexOf(Path.SEPARATOR));

    OutputStream out = new FileOutputStream(localBlockFile);

    try {
      // A HAR part file block could map to several parity files. We need to
      // use all of them to recover this block.
      final long blockEnd = Math.min(blockOffset +
          partFileStat.getBlockSize(),
          partFileStat.getLen());
      for (long offset = blockOffset; offset < blockEnd; ) {
        HarIndex.IndexEntry entry = harIndex.findEntry(partName, offset);
        if (entry == null) {
          String msg = "Lost index file has no matching index entry for " +
          partName + ":" + offset;
          LOG.warn(msg);
          throw new IOException(msg);
        }
        Path parityFile = new Path(entry.fileName);
        int stripeLength = RaidNode.getStripeLength(getConf());
        int paritySizeRS = RaidNode.rsParityLength(getConf());
        int paritySizeSRC = RaidNode.paritySizeSRC(getConf());
        Encoder encoder;
        if (isXorParityFile(parityFile)) {
          encoder = xorEncoder;
        } else if (isRsParityFile(parityFile)) {
          encoder = new ReedSolomonEncoder(getConf(), stripeLength,
              paritySizeRS, 0);
        } else if (isSRCParityFile(parityFile)) {
          encoder = new ReedSolomonEncoder(getConf(), stripeLength,
              paritySizeRS, paritySizeSRC);
        }
        else {
          String msg = "Could not figure out parity file correctly";
          LOG.warn(msg);
          throw new IOException(msg);
        }
        Path srcFile = sourcePathFromParityPath(parityFile);
        FileStatus srcStat = dfs.getFileStatus(srcFile);
        if (srcStat.getModificationTime() != entry.mtime) {
          String msg = "Modification times of " + parityFile + " and " +
          srcFile + " do not match.";
          LOG.warn(msg);
          throw new IOException(msg);
        }
        long lostOffsetInParity = offset - entry.startOffset;
        LOG.info(partFile + ":" + offset + " maps to " +
            parityFile + ":" + lostOffsetInParity +
            " and will be recovered from " + srcFile);
        encoder.recoverParityBlockToStream(dfs, srcFile, srcStat.getLen(),
            srcStat.getBlockSize(), parityFile,
            lostOffsetInParity, out, progress);
        // Finished recovery of one parity block. Since a parity block has the
        // same size as a source block, we can move offset by source block
        // size.
        offset += srcStat.getBlockSize();
        LOG.info("Recovered " + srcStat.getBlockSize() + " part file bytes ");
        if (offset > blockEnd) {
          String msg =
            "Recovered block spills across part file blocks. Cannot continue";
          throw new IOException(msg);
        }
        progress.progress();
      }
    } finally {
      out.close();
    }
  }

  /**
   * Choose a datanode (hostname:portnumber). The datanode is chosen at
   * random from the live datanodes.
   * @param locationsToAvoid locations to avoid.
   * @return A string in the format name:port.
   * @throws IOException
   */
  private String chooseDatanode(DatanodeInfo[] locationsToAvoid)
  throws IOException {
    DistributedFileSystem dfs = getDFS(new Path("/"));
    DatanodeInfo[] live =
      dfs.getClient().datanodeReport(DatanodeReportType.LIVE);
    LOG.info("Choosing a datanode from " + live.length +
        " live nodes while avoiding " + locationsToAvoid.length);
    Random rand = new Random();
    String chosen = null;
    int maxAttempts = 1000;
    for (int i = 0; i < maxAttempts && chosen == null; i++) {
      int idx = rand.nextInt(live.length);
      chosen = live[idx].name;
      for (DatanodeInfo avoid: locationsToAvoid) {
        if (chosen.equals(avoid.name)) {
          LOG.info("Avoiding " + avoid.name);
          chosen = null;
          break;
        }
      }
    }
    if (chosen == null) {
      throw new IOException("Could not choose datanode");
    }
    LOG.info("Choosing datanode " + chosen);
    return chosen;
  }

  /**
   * Reads data from the data stream provided and computes metadata.
   */
  DataInputStream computeMetadata(Configuration conf, InputStream dataStream)
  throws IOException {
    ByteArrayOutputStream mdOutBase = new ByteArrayOutputStream(1024*1024);
    DataOutputStream mdOut = new DataOutputStream(mdOutBase);

    // First, write out the version.
    mdOut.writeShort(FSDataset.METADATA_VERSION);

    // Create a summer and write out its header.
    int bytesPerChecksum = conf.getInt("io.bytes.per.checksum", 512);
    DataChecksum sum =
      DataChecksum.newDataChecksum(DataChecksum.CHECKSUM_CRC32,
          bytesPerChecksum);
    sum.writeHeader(mdOut);

    // Buffer to read in a chunk of data.
    byte[] buf = new byte[bytesPerChecksum];
    // Buffer to store the checksum bytes.
    byte[] chk = new byte[sum.getChecksumSize()];

    // Read data till we reach the end of the input stream.
    int bytesSinceFlush = 0;
    while (true) {
      // Read some bytes.
      int bytesRead = dataStream.read(buf, bytesSinceFlush,
          bytesPerChecksum - bytesSinceFlush);
      if (bytesRead == -1) {
        if (bytesSinceFlush > 0) {
          boolean reset = true;
          sum.writeValue(chk, 0, reset); // This also resets the sum.
          // Write the checksum to the stream.
          mdOut.write(chk, 0, chk.length);
          bytesSinceFlush = 0;
        }
        break;
      }
      // Update the checksum.
      sum.update(buf, bytesSinceFlush, bytesRead);
      bytesSinceFlush += bytesRead;

      // Flush the checksum if necessary.
      if (bytesSinceFlush == bytesPerChecksum) {
        boolean reset = true;
        sum.writeValue(chk, 0, reset); // This also resets the sum.
        // Write the checksum to the stream.
        mdOut.write(chk, 0, chk.length);
        bytesSinceFlush = 0;
      }
    }

    byte[] mdBytes = mdOutBase.toByteArray();
    return new DataInputStream(new ByteArrayInputStream(mdBytes));
  }

  private void computeMetadataAndSendReconstructedBlock(String datanode,
      File localBlockFile,
      Block block, long blockSize)
  throws IOException {

    LOG.info("Computing metdata");
    InputStream blockContents = null;
    DataInputStream blockMetadata = null;
    try {
      blockContents = new FileInputStream(localBlockFile);
      blockMetadata = computeMetadata(getConf(), blockContents);
      blockContents.close();
      // Reopen
      blockContents = new FileInputStream(localBlockFile);
      sendReconstructedBlock(datanode, blockContents, blockMetadata, block,
          blockSize);
    } finally {
      if (blockContents != null) {
        blockContents.close();
        blockContents = null;
      }
      if (blockMetadata != null) {
        blockMetadata.close();
        blockMetadata = null;
      }
    }
  }

  /**
   * Send a generated block to a datanode.
   * @param datanode Chosen datanode name in host:port form.
   * @param blockContents Stream with the block contents.
   * @param block Block object identifying the block to be sent.
   * @param blockSize size of the block.
   * @throws IOException
   */
  private void sendReconstructedBlock(String datanode,
      final InputStream blockContents,
      DataInputStream metadataIn,
      Block block, long blockSize)
  throws IOException {
    InetSocketAddress target = NetUtils.createSocketAddr(datanode);
    Socket sock = SocketChannel.open().socket();

    int readTimeout =
      getConf().getInt(BlockIntegrityMonitor.BLOCKFIX_READ_TIMEOUT,
          HdfsConstants.READ_TIMEOUT);
    NetUtils.connect(sock, target, readTimeout);
    sock.setSoTimeout(readTimeout);

    int writeTimeout = getConf().getInt(BlockIntegrityMonitor.BLOCKFIX_WRITE_TIMEOUT,
        HdfsConstants.WRITE_TIMEOUT);

    OutputStream baseStream = NetUtils.getOutputStream(sock, writeTimeout);
    DataOutputStream out =
      new DataOutputStream(new BufferedOutputStream(baseStream,
          FSConstants.
          SMALL_BUFFER_SIZE));

    boolean corruptChecksumOk = false;
    boolean chunkOffsetOK = false;
    boolean verifyChecksum = true;
    boolean transferToAllowed = false;

    try {
      LOG.info("Sending block " + block +
          " from " + sock.getLocalSocketAddress().toString() +
          " to " + sock.getRemoteSocketAddress().toString());
      BlockSender blockSender =
        new BlockSender(block, blockSize, 0, blockSize,
            corruptChecksumOk, chunkOffsetOK, verifyChecksum,
            transferToAllowed,
            metadataIn, new BlockSender.InputStreamFactory() {
          @Override
          public InputStream createStream(long offset)
          throws IOException {
            // we are passing 0 as the offset above,
            // so we can safely ignore
            // the offset passed
            return blockContents;
          }
        });

      // Header info
      out.writeShort(getDataTransferProtocolVersion(getConf()));
      out.writeByte(DataTransferProtocol.OP_WRITE_BLOCK);
      out.writeLong(block.getBlockId());
      out.writeLong(block.getGenerationStamp());
      out.writeInt(0);           // no pipelining
      out.writeBoolean(false);   // not part of recovery
      Text.writeString(out, ""); // client
      out.writeBoolean(true); // sending src node information
      DatanodeInfo srcNode = new DatanodeInfo();
      srcNode.write(out); // Write src node DatanodeInfo
      // write targets
      out.writeInt(0); // num targets
      // send data & checksum
      blockSender.sendBlock(out, baseStream, null);

      LOG.info("Sent block " + block + " to " + datanode);
    } finally {
      sock.close();
      out.close();
    }
  }

  /**
   * returns the source file corresponding to a parity file
   */
  Path sourcePathFromParityPath(Path parityPath) {
    String parityPathStr = parityPath.toUri().getPath();
    if (parityPathStr.startsWith(xorPrefix)) {
      // Remove the prefix to get the source file.
      String src = parityPathStr.replaceFirst(xorPrefix, "/");
      return new Path(src);
    } else if (parityPathStr.startsWith(rsPrefix)) {
      // Remove the prefix to get the source file.
      String src = parityPathStr.replaceFirst(rsPrefix, "/");
      return new Path(src);
    }
    return null;
  }

  /**
   * Returns the lost blocks in a file.
   */
  abstract List<LocatedBlock> lostBlocksInFile(DistributedFileSystem fs,
      String uriPath, FileStatus stat)
      throws IOException;


  /**
   * This class implements corrupt block fixing functionality.
   */
  public static class CorruptBlockReconstructor extends BlockReconstructor {

    public CorruptBlockReconstructor(Configuration conf) throws IOException {
      super(conf);
    }


    @Override
    List<LocatedBlock> lostBlocksInFile(DistributedFileSystem fs,
                                        String uriPath,
                                        FileStatus stat)
        throws IOException {

      List<LocatedBlock> corrupt = new LinkedList<LocatedBlock>();
      LocatedBlocks locatedBlocks =
        fs.getClient().namenode.getBlockLocations(uriPath, 0, stat.getLen());
      for (LocatedBlock b: locatedBlocks.getLocatedBlocks()) {
        if (b.isCorrupt() ||
            (b.getLocations().length == 0 && b.getBlockSize() > 0)) {
          corrupt.add(b);
        }
      }
      return corrupt;
    }
  }

  /**
   * This class implements decommissioning block copying functionality.
   */
  public static class DecommissioningBlockReconstructor extends BlockReconstructor {

    public DecommissioningBlockReconstructor(Configuration conf) throws IOException {
      super(conf);
    }

    @Override
    List<LocatedBlock> lostBlocksInFile(DistributedFileSystem fs,
                                           String uriPath,
                                           FileStatus stat)
        throws IOException {

      List<LocatedBlock> decommissioning = new LinkedList<LocatedBlock>();
      LocatedBlocks locatedBlocks =
          fs.getClient().namenode.getBlockLocations(uriPath, 0, stat.getLen());

      for (LocatedBlock b : locatedBlocks.getLocatedBlocks()) {
        if (b.isCorrupt() ||
            (b.getLocations().length == 0 && b.getBlockSize() > 0)) {
          // If corrupt, this block is the responsibility of the CorruptBlockReconstructor
          continue;
        }

        // Copy this block iff all good copies are being decommissioned
        boolean allDecommissioning = true;
        for (DatanodeInfo i : b.getLocations()) {
          allDecommissioning &= i.isDecommissionInProgress();
        }
        if (allDecommissioning) {
          decommissioning.add(b);
        }
      }
      return decommissioning;
    }

  }


}
