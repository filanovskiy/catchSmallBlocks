package catchSmallBlocks;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.shell.PathData;

public class SmallBlocksDir {
	
	public static void main(String args[]) throws IOException {
	// Parsing input arguments
	if (args[0].equals("-minBlkSizeMB") && args[2].equals("-minBlkCount") && args[4].equals("-path") )
	{	
		System.out.println("Analizer starting...");
		collectStat(args[1],args[3],args[5]);
		System.out.println("Analizer finished...");
	}
	else
	{
	System.out.println(" Error! Incorrect parameters. Use -minBlkSizeMB for specify minimum block size in Mb that will be showed at the screen");
	System.out.println(" and -minBlkCount for specify minumum blocks that have to be in directory for getting it at the screen");
	System.out.println(" also specify -path for point on the HDFS directory that have to be analyzed ");
	System.out.println(" EXAMPLE:  hadoop jar catchSmallBlocks.jar -minBlkSizeMB 32 -minBlkCount 100 -path / ");
	}
	}
	
	
	public static void collectStat(String minBlkSizeMB,String minBlkCount,String source) throws IOException {
		
		// set hadoop configs
		Configuration conf = new Configuration();
		FileSystem fileSystem = FileSystem.get(conf);
		Path srcPath = new Path(source);
		
		// Check fact of existing alalyzing path
		if (!(ifExists(srcPath))) {
			System.out.println("No such destination " + srcPath);
			return;
		}
		
		// Create iterator for getting all files in directory. Option true means recursive analyze
		RemoteIterator<LocatedFileStatus> fileStatusListIterator = fileSystem.listFiles(srcPath, true);
				
		// define Map structures that will contain information over all directories (Format is: directory, <sumSize, numberOfBlocks>)		
		Map<String, List<Long>> directoryBlock = new TreeMap<>();
		
		// start biggest cycle over root directory, that include all child directories 
		
		while (fileStatusListIterator.hasNext()) {
			
			// fetch next file for analyzing its blocks
			LocatedFileStatus fileStatus = fileStatusListIterator.next();

			// get list of the blocks from that constructed file 
			BlockLocation[] blkLocations = fileStatus.getBlockLocations();
			
			// count number of the blocks
			int blkCount = blkLocations.length;
			
			// cycle across all blocks for some particular file
			// every single iteration adds in array list of the nodes serves that store some particular block
			
						for (int i = 0; i < blkCount; i++) {
							// get parient directory for file. As soon as result we want to have directories -
							// that variable we put in result Map as a key
							String fPath = new File(fileStatus.getPath().toString()).getParent();							
							String key = fPath;
							// intermediate array
						    List<Long> tempValueList = new ArrayList<Long>();
						    // for each key (directory) fetch previous statistics  
						    List<Long> old = directoryBlock.get(key);						    
						    
						    // if it is first appearance
							if (old == null)
							{	
								tempValueList.add(blkLocations[i].getLength());
								tempValueList.add((long) 1);								
								directoryBlock.put(key, tempValueList);
							}
							// if it not first appearance
							else
							{
								tempValueList.add(0, blkLocations[i].getLength() + old.get(0));
								tempValueList.add(1, (Long) old.toArray()[1] + (long) 1);
								directoryBlock.put(key, tempValueList);
							}
						}		
		}
		// print results
		printMap(directoryBlock,minBlkSizeMB,minBlkCount);
}

	
	// checking fact of existing directory for given path
	private static boolean ifExists(Path srcPath) throws IOException {
		Configuration config = new Configuration();		
		FileSystem hdfs = FileSystem.get(config);
		boolean isExists = hdfs.exists(srcPath);
		return isExists;
	}

	// method print results with some particular filters. Filters restrict output	
	public static void printMap(Map<String, List<Long>> mp, String minBlkSizeMB, String minBlkCount) {
	    Iterator it = mp.entrySet().iterator();
	    while (it.hasNext()) {
	        Map.Entry pair = (Map.Entry)it.next();
	        BigInteger[] values = readToArray(pair.getValue().toString());
	        double avgBlockSizeMb = values[0].divide(new BigInteger("1024")).divide(new BigInteger("1024")).divide(values[1]).doubleValue();
	        		//+ values[0].divide(new BigInteger("1024")).divide(new BigInteger("1024")).divideAndRemainder(values[1])[0].doubleValue();
	        double  TotalSizeInGB = values[0].divide(new BigInteger("1024")).divide(new BigInteger("1024")).divide(new BigInteger("1024")).intValue();
	        		//+ values[0].divide(new BigInteger("1024")).divide(new BigInteger("1024")).divideAndRemainder(new BigInteger("1024"))[1].doubleValue();
	        long totalNumBlocks = values[1].intValue();
	    	
	        // print results that meet given requirements
        	if (avgBlockSizeMb < Integer.parseInt(minBlkSizeMB) && totalNumBlocks> Integer.parseInt(minBlkCount))
	        {
	        System.out.println("Directory is: " + pair.getKey() + "|| Size in Gbytes: " + TotalSizeInGB + ", Total blocks: " + totalNumBlocks + ", average block size in Mb: " + avgBlockSizeMb);
	        }
	        it.remove(); // avoids a ConcurrentModificationException
	        
	    }
	}
	
	// read String to array. Using BigInteger because values of bytes for some directories are huge
	public static BigInteger[] readToArray(String line) {
		String[] items = line.replaceAll("\\[", "").replaceAll("\\]", "").replaceAll("\\s+","").split(",");
		BigInteger[] results = new BigInteger[items.length];
		for (int i = 0; i < items.length; i++) {
		    try {
		        results[i] = new  BigInteger(items[i]);
		    } catch (NumberFormatException nfe) {};
		}
		return results;		
	}	 
}
