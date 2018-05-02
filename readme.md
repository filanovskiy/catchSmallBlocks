To build the report run from edge/any hadoop node command:

$ sudo -u hdfs hadoop jar catchSmallBlocks.jar  -minBlkSizeMB 32 -minBlkCount 10000 -path /

Where:
-minBlkSizeMB is the threshould for the block size after this blocks are considering like small. 32MB is good point for start, don't try to start with large numbers, like 200MB - you will have big report, which is hard to analyze.
-minBlkCount is number of small blocks. You may have files, which have few small blocks, it's not good, but not critical.
-path / - HDFS subdirectory for analyzing


For merge files you may use Spark Job. You could check "MergeFilesExample" in this repo

