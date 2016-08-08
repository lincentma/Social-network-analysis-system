This is a small graph example to show the usage of kcore algorithm in X-RIME.
The input raw data is in the file ./data/kcore.dat. The graph is shown in the
figure ./data/kcore.png.

Steps:
1. Copy the example input data file to Hadoop HDFS. The command line will be
like follows:

hadoop fs -copyFromLocal data /usr/nobody/test/kcore/data

2. Since the raw input data is in a proprietary format, we need to transform it
to X-RIME's internal format first. After moving to proper directory, use the
following command:

hadoop jar xrime.jar org.sf.xrime.preprocessing.smth.Raw2OutAdjVertexTransformer \
    /usr/nobody/test/kcore/data/kcore.dat \
    /usr/nobody/test/kcore/out_adj_vertex

3. Then run the algorithm. Suppose we want to extract 3-core. Use the following
command:

hadoop jar xrime.jar org.sf.xrime.algorithms.kcore.undirected.SpecifiedKCoreAlgorithm \ 
    -k 3 /usr/nobody/test/kcore/out_adj_vertex /usr/nobody/test/kcore/3_core

The option "-k 3" means we want to extract 3-core.

4. Check out the result.

hadoop fs -cat /usr/nobody/test/kcore/3_core/*

The vertexes belong to 3-core are: V5, V7, V10, V11, V12, V15, V16, V17.
