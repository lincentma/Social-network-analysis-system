This is a small graph example to show the usage of hits algorithm in X-RIME.
The input data is in the file ./data/hits.dat.

Steps:
1. Copy the example input data file to Hadoop HDFS. The command line will be
like follows:

hadoop fs -copyFromLocal data /usr/nobody/test/hits/data

2. Since the input data is in a proprietary format, we need to transform it
to X-RIME's internal format first. After moving to proper directory, use the
following command:

hadoop jar xrime.jar org.sf.xrime.preprocessing.pajek.PajekFormat2LabeledAdjBiSetVertex \
	 /usr/nobody/test/hits/data/hits.dat \
    /usr/nobody/test/hits/input

	
3. Then run the algorithm. Use the following command:

hadoop jar xrime.jar org.sf.xrime.algorithms.HITS.Runner \ 
    /usr/nobody/test/hits/input
    
4. We need to transform the output to text for human reading.

hadoop jar xrime.jar org.sf.xrime.postprocessing.SequenceFileToTextFileTransformer \
    /usr/nobody/test/hits/inputOut* 
    
5. Check out the result.

hadoop fs -cat /usr/nobody/test/hits/inputOut*Text/*

The result is:
A	<A, <B>, <D>>, <<<xrime.algorithm.HITS.Authority, <0.011,0.007>>, <xrime.algorithm.HITS.Hub, <0.374,0.377>>>>
B	<B, <C>, <A, C>>, <<<xrime.algorithm.HITS.Authority, <0.604,0.610>>, <xrime.algorithm.HITS.Hub, <0.011,0.007>>>>
C	<C, <D, B>, <B>>, <<<xrime.algorithm.HITS.Authority, <0.011,0.007>>, <xrime.algorithm.HITS.Hub, <0.604,0.610>>>>
D	<D, <A>, <C>>, <<<xrime.algorithm.HITS.Authority, <0.374,0.377>>, <xrime.algorithm.HITS.Hub, <0.011,0.007>>>>
E	<E, <>, <>>, <<<xrime.algorithm.HITS.Authority, <0.000,0.000>>, <xrime.algorithm.HITS.Hub, <0.000,0.000>>>>

That is:
(1)hub score of each vertex:
A:0.377
B:0.007
C:0.610
D:0.007
E:0.000
(2)authority score of each vertex:
A:0.007
B:0.610
C:0.007
D:0.377
E:0.000