This is a small graph example to show the usage of ellipse layout algorithm 
in X-RIME. The input raw data is in the file ./data/ellipse.dat. The result 
graph is shown in the figure ./data/ellipse.png.

Steps:
1. Copy the example input data file to Hadoop HDFS. The command line will be
like follows:

hadoop fs -copyFromLocal data /usr/nobody/test/ellipse/data

2. Since the raw input data is in a proprietary format, we need to transform it
to X-RIME's internal format first. After moving to proper directory, use the
following command:

hadoop jar xrime.jar org.sf.xrime.preprocessing.smth.Raw2OutAdjVertexTransformer \
    /usr/nobody/test/ellipse/data/ellipse.dat \
    /usr/nobody/test/ellipse/out_adj_vertex

3. Then run the algorithm. Suppose we want to use 600*400 as a display area. Use 
the following command:

hadoop jar xrime.jar org.sf.xrime.algorithms.layout.ellipse.EllipseLayoutAlgorithm \ 
    /usr/nobody/test/ellipse/out_adj_vertex 
    /usr/nobody/test/ellipse/result  
    -x 600 -y 400 

The option "-x 600" means we want to use 600 as the length of display.
The option "-y 400" means we want to use 400 as the width of display.

4. Check out the result.The final result file is named as ellispe.

hadoop fs -cat /usr/nobody/test/ellipse/result/ellipse

5. Draw the resul graph. We use a python script to draw a svg graph from the final
result file. 

python draw_from_file /usr/nobody/test/ellipse/result/ellipse/part-00000 600 400

The first option "600" means we want to use 600 as the length of display.
The second option "400" means we want to use 400 as the width of display.

6. Check out the result graph. We use ImageMagick to display the result graph.

display /usr/nobody/test/ellipse/result/ellipse/part-0000.svg 600 400

The first option "600" means we want to use 600 as the length of display.
The second option "400" means we want to use 400 as the width of display.
