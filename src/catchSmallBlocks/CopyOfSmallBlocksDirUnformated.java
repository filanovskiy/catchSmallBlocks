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

public class CopyOfSmallBlocksDirUnformated {
	
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
		
		// Get the filename out of the file path
		//String filename = source.substring(source.lastIndexOf('/') + 1, source.length());
		
		// Create iterator for getting all files in directory
		RemoteIterator<LocatedFileStatus> fileStatusListIterator = fileSystem.listFiles(srcPath, true);
				

		
		// define structures that will contain information over all directories		
		Map<String, List<Long>> directoryBlock = new TreeMap<>();
		
		// start biggest cycle over root directory
		while (fileStatusListIterator.hasNext()) {
			
			//Path path = fileStatusListIterator.next().getPath();
			LocatedFileStatus fileStatus = fileStatusListIterator.next();
			//Path path = fileStatus;
			BlockLocation[] blkLocations = fileStatus.getBlockLocations();
			int blkCount = blkLocations.length;

			List<String> allhosts = new ArrayList<String>();
			List<Long> blocksize = new ArrayList<Long>();			 
			// cycle across all blocks. every single iteration adds in array list of the nodes serves that store some particular block
			
						for (int i = 0; i < blkCount; i++) {
							String fPath = new File(fileStatus.getPath().toString()).getParent();
							//directoryBlock.put(fPath,blkLocations[i].getLength());
							String key = fPath;
						    List<Long> tempValueList = new ArrayList<Long>();
						    // add at [0] possition byte value
						    
						    
						    List<Long> old = directoryBlock.get(key);						    
						    //System.out.println("OLD is:" + old);
						    
							if (old == null)
							{	
								tempValueList.add(blkLocations[i].getLength());
								tempValueList.add((long) 1);								
								directoryBlock.put(key, tempValueList);
							}
							else
							{
								//Long tempEx = (Long) tempValueList.toArray()[0];
								tempValueList.add(0, blkLocations[i].getLength() + old.get(0));
								tempValueList.add(1, (Long) old.toArray()[1] + (long) 1);
								directoryBlock.put(key, tempValueList);
							}
						}
						//System.out.println("Map is:" + directoryBlock);
		}
			
		//System.out.println("Average block size for each directory is : " + directoryBlock);
		printMap(directoryBlock,minBlkSizeMB,minBlkCount);
		//incrementValue(directoryBlock,directoryBlock.get(key))
}

	
	private static boolean ifExists(Path srcPath) throws IOException {
		Configuration config = new Configuration();		
		FileSystem hdfs = FileSystem.get(config);
		boolean isExists = hdfs.exists(srcPath);
		return isExists;
	}

	private static double calculateAverage(List<Long> marks) {
		if (marks == null || marks.isEmpty()) {
			return 0;
		}
		double sum = 0;
		for (Long mark : marks) {
			sum += mark;
		}
		return sum / marks.size();
	}
	
	
	public static void printMap(Map mp, String minBlkSizeMB, String minBlkCount) {
	    Iterator it = mp.entrySet().iterator();
	    while (it.hasNext()) {
	        Map.Entry pair = (Map.Entry)it.next();
	        BigInteger[] values = null;
	        //System.out.println(pair.getValue().toString());
	        values = readToArray(pair.getValue().toString());
	        
	        //double avgBlockSizeKb = (double) values[0]/1024/values[1] ;
	        //double TotalSizeInMB = values[0]/1024/1024;
	        //int totalNumBlocks = values[1];

	        double avgBlockSizeMb = values[0].divide(new BigInteger("1024")).divide(new BigInteger("1024")).divide(values[1]).doubleValue();
	        		//+ values[0].divide(new BigInteger("1024")).divide(new BigInteger("1024")).divideAndRemainder(values[1])[0].doubleValue();
	        double  TotalSizeInGB = values[0].divide(new BigInteger("1024")).divide(new BigInteger("1024")).divide(new BigInteger("1024")).intValue();
	        		//+ values[0].divide(new BigInteger("1024")).divide(new BigInteger("1024")).divideAndRemainder(new BigInteger("1024"))[1].doubleValue();
	        long totalNumBlocks = values[1].intValue();
	    	
        	if (avgBlockSizeMb < Integer.parseInt(minBlkSizeMB) && totalNumBlocks> Integer.parseInt(minBlkCount))
	        {
	        System.out.println("Directory is: " + pair.getKey() + "|| Size in Gbytes: " + TotalSizeInGB + ", Total blocks: " + totalNumBlocks + ", average block size in Mb: " + avgBlockSizeMb);
	        //	System.out.println(pair.getValue().toString());
	        //	System.out.println(values.toString());
	        //	System.out.println(values[0]);
	        //	System.out.println(values[1]);
	        }
	        it.remove(); // avoids a ConcurrentModificationException
	        
	    }
	}
	
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
