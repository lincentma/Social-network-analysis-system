This is a small graph example to show the usage of weakly-connected component
algorithm in X-RIME. The input raw data is in the file ./data/wcc.dat. The graph
is shown in the figure ./data/wcc.png (the same graph is also used to show the
k-core algorithm).

Steps:
1. Copy the example input data file to Hadoop HDFS. The command line will be
like follows:

hadoop fs -copyFromLocal data /usr/nobody/test/wcc/data

2. Since the raw input data is in a proprietary format, we need to transform it
to X-RIME's internal format first. After moving to proper directory, use the
following command:

hadoop jar xrime.jar org.sf.xrime.preprocessing.smth.Raw2OutAdjVertexTransformer \
    /usr/nobody/test/wcc/data/wcc.dat \
    /usr/nobody/test/wcc/out_adj_vertex

3. Then run the algorithm. Use the following command:

hadoop jar xrime.jar org.sf.xrime.algorithms.partitions.connected.weakly.alg_2.TraversalBasedAlgorithm \ 
    /usr/nobody/test/wcc/out_adj_vertex /usr/nobody/test/wcc/wccs

4. Check out the result.

hadoop fs -cat /usr/nobody/test/wcc/wccs/*

The result is a mapping from vertexes to the ID of their belonging WCC.
