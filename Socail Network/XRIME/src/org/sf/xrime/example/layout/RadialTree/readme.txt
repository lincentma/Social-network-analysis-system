This is a small graph example to show the usage of radialtree layout algorithm 
in X-RIME. The input raw data is in the file ./data/RadialTree.dat. The result 
graph is shown in the figure ./data/RadialTree.png.

Steps:
1. Copy the example input data file to Hadoop HDFS. The command line will be
like follows:

hadoop fs -copyFromLocal data /usr/nobody/test/RadialTree/data

2. Since the raw input data is in a proprietary format, we need to transform it
to X-RIME's internal format first. After moving to proper directory, use the
following command:

hadoop jar xrime.jar org.sf.xrime.algorithms.layout.radialtree.Raw2OutLabeledAdjVertexTransformer \
    /usr/nobody/test/RadialTree/data/RadialTree.dat \
    /usr/nobody/test/RadialTree/out_labeled_adj_vertex

3. Then run the algorithm. Suppose we want to use 700*700 as a display area and 
vertex 'A' as root vertex. Use the following command:

hadoop jar xrime.jar org.sf.xrime.algorithms.layout.radialtree.RadialTreeRunner \ 
    /usr/nobody/test/RadialTree/out_labeled_adj_vertex -x 700 -y 700 -id A

The option "-x 700" means we want to use 700 as the length of display.
The option "-y 700" means we want to use 700 as the width of display.
The option "-id A" means we want to use vertex 'A' as root vertex.

4. Check out the result.The last file is the final result.

hadoop fs -cat /usr/nobody/RadialTree/Out*

5. Draw the resul graph. We use a python script to draw a svg graph from the final
result file. We support two scripts to draw the graph with vertex label or not.

python draw_from_file_usingRadialTree_nolabel /usr/nobody/RadialTree/Out_last 700 700
python draw_from_file_usingRadialTree /usr/nobody/RadialTree/Out_last 700 700

The first option "700" means we want to use 700 as the length of display.
The second option "700" means we want to use 700 as the width of display.

6. Check out the result graph. We use ImageMagick to display the result graph.

display /usr/nobody/RadialTree/Out_last.svg 700 700

The first option "700" means we want to use 700 as the length of display.
The second option "700" means we want to use 700 as the width of display.
