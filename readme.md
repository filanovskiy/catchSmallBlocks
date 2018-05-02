To build the report run from edge/any hadoop node command:

$ sudo -u hdfs hadoop jar catchSmallBlocks.jar  -minBlkSizeMB 32 -minBlkCount 10000 -path /

Where:
-minBlkSizeMB is the threshould for the block size after this blocks are considering like small. 32MB is good point for start, don't try to start with large numbers, like 200MB - you will have big report, which is hard to analyze.
-minBlkCount is number of small blocks. You may have files, which have few small blocks, it's not good, but not critical.
-path / - HDFS subdirectory for analyzing
------------------------------------------------------------------
For merge files you may use Spark Job. Here is the example of Spark code for merge Small Blocks.
------------------------------------------------------------------

$ spark2-shell
…
// Generate some files with small block

import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import util.Random
val fs=FileSystem.get(sc.hadoopConfiguration)
val SmallBlockDir="/tmp/generated_data_smallblock"
if(fs.exists(new Path(SmallBlockDir)))
  fs.delete(new Path(SmallBlockDir),true)
val numRecords:Int = 1000000000
val partitions:Int = 1000
val recordsPerPartition = numRecords / partitions
val seedRdd = sc.parallelize(Seq.fill(partitions)(recordsPerPartition),partitions)
val randomNrs = seedRdd.flatMap(records => Seq.fill(records)(Random.nextInt))
randomNrs.saveAsTextFile(SmallBlockDir)

// check block size

$ hdfs fsck /tmp/generated_data_smallblock
…..
Total blocks (validated):      1000 (avg. block size 10982611 B)
….

// write compactor

$spark2-shell
…
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import util.Random
val fs=FileSystem.get(sc.hadoopConfiguration)
val SmallBlockDir="/tmp/generated_data_smallblock"
val BigBlockDir="/tmp/generated_data_bigblock"
val hdfs: org.apache.hadoop.fs.FileSystem = org.apache.hadoop.fs.FileSystem.get(new org.apache.hadoop.conf.Configuration())
if(fs.exists(new Path(BigBlockDir)))
  fs.delete(new Path(BigBlockDir),true)
val hadoopPath= new org.apache.hadoop.fs.Path(SmallBlockDir)
val recursive = false
val ri = hdfs.listFiles(hadoopPath, recursive)
val it = new Iterator[org.apache.hadoop.fs.LocatedFileStatus]() {
  override def hasNext = ri.hasNext
  override def next() = ri.next()
}
val files = it.toList
val dirSize = files.map(_.getLen).sum
val blockSize = 1024 * 1024 * 255
sc.hadoopConfiguration.setInt( "dfs.blocksize", blockSize )
sc.hadoopConfiguration.setInt( "parquet.block.size", blockSize )
val numFiles = dirSize/blockSize
val bigFile=sc.textFile(SmallBlockDir).repartition(numFiles.toInt)
bigFile.saveAsTextFile(BigBlockDir)

// Check block size after

$ hdfs fsck /tmp/generated_data_bigblock
…
 Total blocks (validated):      41 (avg. block size 267868112 B)
…
