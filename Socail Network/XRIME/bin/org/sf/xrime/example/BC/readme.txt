This is a small graph to show the usage of BC Algorithm in X-Rime.
The input is in the file ./data/bc.dat

Steps:
1. Copy the example input data file to Hadoop HDFS. The command line will be
like follows:
hadoop fs -copyFromLocal data /usr/nobody/test/bc/data

2.Then run the algorithm. Use the following command:
hadoop jar xrime.jar org.sf.xrime.algorithms.BC.BCRunner  /usr/nobody/test/bc/input /usr/nobody/test/bc/out /usr/nobody/test/bc/shuchu a

3. The final result of the whole BC algorithm is printed on the console 

the result is:

the bc of vertex   a   is   35.333336

