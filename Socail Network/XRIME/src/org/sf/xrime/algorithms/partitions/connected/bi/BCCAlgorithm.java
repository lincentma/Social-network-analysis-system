/*
 * Copyright (C) IBM Corp. 2009.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.sf.xrime.algorithms.partitions.connected.bi;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Stack;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.ToolRunner;
import org.sf.xrime.ProcessorExecutionException;
import org.sf.xrime.Transformer;
import org.sf.xrime.algorithms.GraphAlgorithm;
import org.sf.xrime.algorithms.transform.vertex.OutAdjVertex2AdjSetVertexTransformer;
import org.sf.xrime.algorithms.transform.vertex.Vertex2LabeledTransformer;
import org.sf.xrime.model.Graph;
import org.sf.xrime.model.vertex.LabeledAdjSetVertex;
import org.sf.xrime.utils.MRConsoleReader;
import org.sf.xrime.utils.SequenceTempDirMgr;


/**
 * Input: a connected graph represented as outgoing adjacency lists.
 * Output: partition of the edge set, which represents the BCCs of the graph.
 * @author xue
 */
public class BCCAlgorithm extends GraphAlgorithm {
  /**
   * Default constructor.
   */
  public BCCAlgorithm(){
    super();
  }
  
  @Override
  public void setArguments(String[] params) throws ProcessorExecutionException {
    // Make sure there are exactly 2 parameters left.
    if (params.length != 2) {
      throw new ProcessorExecutionException("Wrong number of parameters: " +
                         params.length + " instead of 2.");
    }

    // Configure the algorithm instance.
    Graph src = new Graph(Graph.defaultGraph());
    src.setPath(new Path(params[0]));
    Graph dest = new Graph(Graph.defaultGraph());
    dest.setPath(new Path(params[1]));
    
    setSource(src);
    setDestination(dest);
  }


  @Override
  public void execute() throws ProcessorExecutionException {
    try {
      if(getSource().getPaths()==null||getSource().getPaths().size()==0||
          getDestination().getPaths()==null||getDestination().getPaths().size()==0){
        throw new ProcessorExecutionException("No input and/or output paths specified.");
      }
      
      // The prefix used by temp directories which store intermediate results of each steps.
      String temp_dir_prefix = getDestination().getPath().getParent().toString()+"/bcc_"+
                          getDestination().getPath().getName()+"_";
      
      // Create the temporary directory manager.
      SequenceTempDirMgr dirMgr = new SequenceTempDirMgr(temp_dir_prefix, context);
      // Sequence number begins with zero.
      dirMgr.setSeqNum(0);
      Path tmpDir;
    
      // 1. Transform input from outgoing adjacency vertexes lists to AdjSetVertex.
      System.out.println("++++++>"+dirMgr.getSeqNum()+": Transform input to AdjSetVertex");
      Transformer transformer = new OutAdjVertex2AdjSetVertexTransformer();
      transformer.setConf(context);
      transformer.setSrcPath(getSource().getPath());
      // Generate temporary directory.
      tmpDir = dirMgr.getTempDir();
      // And use it as the destination directory.
      transformer.setDestPath(tmpDir);
      transformer.setMapperNum(getMapperNum());
      transformer.setReducerNum(getReducerNum());
      transformer.execute();
      
      // 2. Add initial labels (empty) to AdjSetVertex.
      System.out.println("++++++>"+dirMgr.getSeqNum()+": Transform input to LabeledAdjSetVertex");
      Vertex2LabeledTransformer l_transformer = new Vertex2LabeledTransformer();
      l_transformer.setConf(context);
      l_transformer.setSrcPath(tmpDir);
      // Generate temporary directory.
      tmpDir = dirMgr.getTempDir();
      // And use it as the destination directory.
      l_transformer.setDestPath(tmpDir);
      l_transformer.setMapperNum(getMapperNum());
      l_transformer.setReducerNum(getReducerNum());
      // Don't forget this.
      l_transformer.setOutputValueClass(LabeledAdjSetVertex.class);
      l_transformer.execute();
      
      
      Graph src;
      Graph dest;
      
      // 3. Generate the spanning tree.
      // 3.1 Choose root for the spanning tree.
      // Remember the path.
      Path path_to_remember = tmpDir;
      System.out.println("++++++>"+dirMgr.getSeqNum()+": SpanningTreeRootChoose");
      src = new Graph(Graph.defaultGraph()); 
      src.setPath(tmpDir);
      dest = new Graph(Graph.defaultGraph());
      tmpDir = dirMgr.getTempDir();
      dest.setPath(tmpDir);
      GraphAlgorithm choose_root = new SpanningTreeRootChoose();
      choose_root.setConf(context);
      choose_root.setSource(src);
      choose_root.setDestination(dest);
      choose_root.setMapperNum(getMapperNum());
      choose_root.setReducerNum(getReducerNum());
      choose_root.execute();
      
      // 3.2 Read the id of the chosen root vertex.
      // Determine the file path.
      Path the_file = new Path(tmpDir.toString()+"/part-00000");
      // Get the file system object.
      FileSystem client = FileSystem.get(context);
      // Check for existence.
      if(!client.exists(the_file)){
        throw new ProcessorExecutionException("Did not find the chosen vertex in "+the_file.toString());
      }
      // Open the file for read.
      FSDataInputStream input_stream = client.open(the_file);
      ByteArrayOutputStream output_stream = new ByteArrayOutputStream();
      // Copy to output.
      IOUtils.copyBytes(input_stream, output_stream, context, false);
      // Get the line.
      String the_line = output_stream.toString();
      String root_vertex_id = the_line.substring(SpanningTreeRootChoose.SPANNING_TREE_ROOT.length()).trim();
      // Close the input and output stream.
      input_stream.close();
      output_stream.close();
      System.out.println("++++++> Chosen the root of spanning tree = " + root_vertex_id);
      
      // 3.3 Generate the spanning tree root at chosen vertex.
      while(true){
        // 3.3.1 Spanning.
        System.out.println("++++++>" + dirMgr.getSeqNum()+" Generate the spanning tree rooted at : (" + 
            root_vertex_id +") from " + tmpDir);
        src = new Graph(Graph.defaultGraph());
        src.setPath(path_to_remember);
        tmpDir = dirMgr.getTempDir();
        dest = new Graph(Graph.defaultGraph());
        dest.setPath(tmpDir);
        
        // Remember the directory again.
        path_to_remember = tmpDir;
        
        GraphAlgorithm spanning = new SpanningTreeGenerate();
        spanning.setConf(context);
        spanning.setSource(src);
        spanning.setDestination(dest);
        spanning.setMapperNum(getMapperNum());
        spanning.setReducerNum(getReducerNum());
        spanning.setParameter(ConstantLabels.ROOT_ID, root_vertex_id);
        spanning.execute();
        
        // 3.3.2 Check for convergence.
        System.out.println("++++++>"+ dirMgr.getSeqNum()+" Test spanning convergence");
        src = new Graph(Graph.defaultGraph());
        src.setPath(tmpDir);
        tmpDir = dirMgr.getTempDir();
        dest = new Graph(Graph.defaultGraph());
        dest.setPath(tmpDir);
        
        GraphAlgorithm conv_tester = new SpanningConvergenceTest();
        conv_tester.setConf(context);
        conv_tester.setSource(src);
        conv_tester.setDestination(dest);
        conv_tester.setMapperNum(getMapperNum());
        conv_tester.setReducerNum(getReducerNum());
        conv_tester.execute();
        
        // 3.3.3 Check test result.
        long vertexes_out_of_tree = MRConsoleReader.getMapOutputRecordNum(conv_tester.getFinalStatus());
        System.out.println("++++++> number of vertexes out of the spanning tree = " + vertexes_out_of_tree);
        if(vertexes_out_of_tree == 0){
          // Converged.
          break;
        }
      }
      
      // Here we should have already got a spanning tree.
      
      // 4. From the spanning tree to sets of edges, where each set corresponds to a circle in the graph.
      System.out.println("++++++> From spanning tree to sets of edges");
      src = new Graph(Graph.defaultGraph());
      src.setPath(path_to_remember);
      tmpDir = dirMgr.getTempDir();
      dest = new Graph(Graph.defaultGraph());
      dest.setPath(tmpDir);
      
      GraphAlgorithm tree2set = new Tree2EdgeSet();
      tree2set.setConf(context);
      tree2set.setSource(src);
      tree2set.setDestination(dest);
      tree2set.setMapperNum(getMapperNum());
      tree2set.setReducerNum(getReducerNum());
      tree2set.execute();
      
      
      long map_input_records_num = -1; 
      long map_output_records_num = -2;
      Stack<Path> expanding_stack = new Stack<Path>();
      // 5. loop of MinorJoin + Join.
      do{
        // 5.1 MinorJoin.
        System.out.println("++++++>" + dirMgr.getSeqNum()+": EdgeSetMinorJoin");
        GraphAlgorithm minorjoin = new EdgeSetMinorJoin();
        minorjoin.setConf(context);
        src = new Graph(Graph.defaultGraph());
        // Use the output directory of last step as the input directory of this step.
        src.setPath(tmpDir);
        dest = new Graph(Graph.defaultGraph());
        // Generate a new temporary directory.
        tmpDir = dirMgr.getTempDir();
        dest.setPath(tmpDir);
        minorjoin.setSource(src);
        minorjoin.setDestination(dest);
        minorjoin.setMapperNum(getMapperNum());
        minorjoin.setReducerNum(getReducerNum());
        minorjoin.execute();
        
        // Push the minor join result to the stack. This directory will be used in the
        // future to determine the partition on edge set.
        expanding_stack.push(tmpDir);
        
        // 5.2 Join.
        System.out.println("++++++>" + dirMgr.getSeqNum() + ": EdgeSetJoin");
        GraphAlgorithm join = new EdgeSetJoin();
        join.setConf(context);
        src = new Graph(Graph.defaultGraph());
        src.setPath(tmpDir);
        dest = new Graph(Graph.defaultGraph());
        tmpDir = dirMgr.getTempDir();
        dest.setPath(tmpDir);
        join.setSource(src);
        join.setDestination(dest);
        join.setMapperNum(getMapperNum());
        join.setReducerNum(getReducerNum());
        join.execute();
        
        // 5.3 Check for convergence.
        map_input_records_num = MRConsoleReader.getMapInputRecordNum(join.getFinalStatus());
        map_output_records_num = MRConsoleReader.getMapOutputRecordNum(join.getFinalStatus());
        System.out.println("++++++> map in/out : " + map_input_records_num +"/" + map_output_records_num);
      }while(map_input_records_num!=map_output_records_num);
      
      // 6. Expand, i.e., propagate label to determine the partition on edge set.
      while(expanding_stack.size()>0){
        // 6.1 Expand
        System.out.println("++++++>" + dirMgr.getSeqNum()+": EdgeSetExpand");
        GraphAlgorithm expand = new EdgeSetExpand();
        expand.setConf(context);
        src = new Graph(Graph.defaultGraph());
        src.addPath(expanding_stack.pop());
        src.addPath(tmpDir);
        dest = new Graph(Graph.defaultGraph());
        tmpDir = dirMgr.getTempDir();
        dest.setPath(tmpDir);
        expand.setSource(src);
        expand.setDestination(dest);
        expand.setMapperNum(getMapperNum());
        expand.setReducerNum(getReducerNum());
        expand.execute();
        
        // 6.2 MinorExpand
        System.out.println("++++++>" + dirMgr.getSeqNum() + ": EdgeSetMinorExpand");
        GraphAlgorithm minorexpand = new EdgeSetMinorExpand();
        minorexpand.setConf(context);
        src = new Graph(Graph.defaultGraph());
        src.setPath(tmpDir);
        dest = new Graph(Graph.defaultGraph());
        tmpDir = dirMgr.getTempDir();
        dest.setPath(tmpDir);
        minorexpand.setSource(src);
        minorexpand.setDestination(dest);
        minorexpand.setMapperNum(getMapperNum());
        minorexpand.setReducerNum(getReducerNum());
        minorexpand.execute();
      }
      
      // 7. Summarize sets.
      System.out.println("++++++>"+dirMgr.getSeqNum()+": EdgeSetSummarize");
      GraphAlgorithm summarize = new EdgeSetSummarize();
      summarize.setConf(context);
      src = new Graph(Graph.defaultGraph());
      src.setPath(tmpDir);
      dest = new Graph(Graph.defaultGraph());
      dest.setPath(getDestination().getPath());
      summarize.setSource(src);
      summarize.setDestination(dest);
      summarize.setMapperNum(getMapperNum());
      summarize.setReducerNum(getReducerNum());
      summarize.execute();
      
      // Delete all temporary directories.
      dirMgr.deleteAll();
    }catch(IOException e){
      throw new ProcessorExecutionException(e);
    } catch (IllegalAccessException e) {
      throw new ProcessorExecutionException(e);
    }
  }
  
  public static void main(String[] args){
    try {
      int res = ToolRunner.run(new BCCAlgorithm(), args);
      System.exit(res);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
