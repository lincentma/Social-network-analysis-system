This is a small graph example to show the usage of strongly connected component
algorithm in X-RIME. The input raw data is in the file ./data/scc.dat. The graph
is shown in the figure ./data/scc.png (the left figure in the file).

Steps:
1. Copy the example input data file to Hadoop HDFS. The command line will be
like follows:

hadoop fs -copyFromLocal data /usr/nobody/test/scc/data

2. Since the raw input data is in a proprietary format, we need to transform it
to X-RIME's internal format first. After moving to proper directory, use the
following command:

hadoop jar xrime.jar org.sf.xrime.preprocessing.smth.Raw2InAdjVertexTransformer \
    /usr/nobody/test/scc/data/scc.dat \
    /usr/nobody/test/scc/in_adj_vertex

3. Then run the algorithm. Use the following command:

hadoop jar xrime.jar org.sf.xrime.algorithms.partitions.connected.strongly.SCCAlgorithm \ 
    /usr/nobody/test/scc/in_adj_vertex /usr/nobody/test/scc/sccs

4. Check out the result.

hadoop fs -cat /usr/nobody/test/scc/sccs/*

The result is a mapping from vertexes to their belonging SCC. For example, vertexes V4, V5
and V6 belong to the same SCC, which is identified using the ID V4.
