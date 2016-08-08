This is a small graph example to show the usage of bi-connected component
algorithm in X-RIME. The input raw data is in the file ./data/bcc.dat. The graph
is shown in the figure ./data/bcc.png.

Steps:
1. Copy the example input data file to Hadoop HDFS. The command line will be
like follows:

hadoop fs -copyFromLocal data /usr/nobody/test/bcc/data

2. Since the raw input data is in a proprietary format, we need to transform it
to X-RIME's internal format first. After moving to proper directory, use the
following command:

hadoop jar xrime.jar org.sf.xrime.preprocessing.smth.Raw2OutAdjVertexTransformer \
    /usr/nobody/test/bcc/data/bcc.dat \
    /usr/nobody/test/bcc/out_adj_vertex

3. Then run the algorithm. Use the following command:

hadoop jar xrime.jar org.sf.xrime.algorithms.partitions.connected.bi.BCCAlgorithm \ 
    /usr/nobody/test/bcc/out_adj_vertex /usr/nobody/test/bcc/bccs

4. We need to transform the output to text for human reading.

hadoop jar xrime.jar org.sf.xrime.postprocessing.SequenceFileToTextFileTransformer \
    /usr/nobody/test/bcc/bccs /usr/nobody/test/bcc/bccs_txt

5. Check out the result.

hadoop fs -cat /usr/nobody/test/bcc/bccs_txt/*

The result is represented as sets of edges. This is because that bi-connected components
are partitions of the edge set of a graph.

