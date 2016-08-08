This is a small graph example to show the usage of MST algorithm in X-RIME.
The input raw data is in the file ./data/mst.dat. The original graph and the 
final result graph are shown in the figure ./data/mst.png.

Steps:
1. Use the preprocessing tool to transfer the example input to the format needed
by MST algorithm. The command line will be like follows:

hadoop jar xrime.jar org.sf.xrime.preprocessing.pajek.PajekFormat2WeightedLabeledAdjVertex \
			/usr/nobody/test/mst/data/mst.dat \
			/usr/nobody/test/mst/data/Input 

2. Copy the example input data file to Hadoop HDFS. The command line will be
like follows:

		hadoop fs -copyFromLocal data /usr/nobody/test/mst/Input

3. Then run the algorithm. 
			
			attention:-i can be used to set the vertexes to wake up automatically. If -i is not used, all
			vertexes will wake up automatically by default
			
			The command line will be like follows:
			hadoop jar xrime.jar org.sf.xrime.algorithms.MST.MSTRunner \
			/usr/nobody/test/mst/Input -i A C D
			
4. We need to transform the output to text for human reading.

			hadoop jar xrime.jar org.sf.xrime.postprocessing.SequenceFileToTextFileTransformer \
    	/usr/nobody/test/mst/FinalOutput

5. Check out the result.

			hadoop fs -cat /usr/nobody/test/mst/FinalOutputText/*
			
			We can see "xrime.algorithm.MST.edge.states.label" of vertex 'A' from the output like 
			the following:
					<xrime.algorithm.MST.edge.states.label, <<D, 0>, <B, -1>, <C, -1>>>
					We can see that the state for vertex 'C' is -1 which means that C is vertex A's neighbor
					in the minimum spanning tree, vertexes whose value are not -1 are not vertex A's neighbor
					