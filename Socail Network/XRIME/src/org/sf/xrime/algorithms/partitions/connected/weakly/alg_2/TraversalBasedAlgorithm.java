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
package org.sf.xrime.algorithms.partitions.connected.weakly.alg_2;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.RunningJob;
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
 * This class wraps all steps used to determine weakly connected components. We assume the input
 * is a graph represented as outgoing adjacency vertexes lists.
 * @author xue
 *
 */
public class TraversalBasedAlgorithm extends GraphAlgorithm {
  /** Default constructor.*/
  public TraversalBasedAlgorithm(){
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
      String temp_dir_prefix = getDestination().getPath().getParent().toString()+"/wcc_alg2_"+
                        System.currentTimeMillis()+"_"+getDestination().getPath().getName()+
                        "_";
      
      // Create the temporary directory manager.
      SequenceTempDirMgr dirMgr = new SequenceTempDirMgr(temp_dir_prefix, context);
      // Sequence number begins with zero.
      dirMgr.setSeqNum(0);
      Path tmpDir;
    
      // 1.1 Transform input from outgoing adjacency vertexes lists to AdjSetVertex.
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
      
      // 1.2 Add initial labels to AdjSetVertex.
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
      // Set the class used to add initial labels.
      l_transformer.setLabelAdderClass(InitialLabelsAdder.class);
      // Don't forget this.
      l_transformer.setOutputValueClass(LabeledAdjSetVertex.class);
      l_transformer.execute();
      
      /**
      RunningJob trans_result = l_transformer.getFinalStatus();
      // Get the number of vertexes.
      long vertex_num = MRConsoleReader.getMapOutputRecordNum(trans_result);
      */
      
      // 2. Propagation + Convergence Test within a loop.
      long changed_labels = -1;
      // Use to remember the path to the result of last propagation step.
      Path last_propagation_path = tmpDir;
      do{
        // 2.1 Propagation.
        System.out.println("++++++>"+dirMgr.getSeqNum()+": VertexLabelPropagation");
        GraphAlgorithm algorithm_propagation = new VertexLabelPropagation();
        algorithm_propagation.setConf(context);
        Graph src = new Graph(Graph.defaultGraph());
        // Use the output directory of last step as the input directory of this step.
        src.setPath(last_propagation_path);
        Graph dest = new Graph(Graph.defaultGraph());
        // Generate a new temporary directory.
        tmpDir = dirMgr.getTempDir();
        dest.setPath(tmpDir);
        last_propagation_path = tmpDir;
        algorithm_propagation.setSource(src);
        algorithm_propagation.setDestination(dest);
        algorithm_propagation.setMapperNum(getMapperNum());
        algorithm_propagation.setReducerNum(getReducerNum());
        algorithm_propagation.execute();
        
        // 2.2 Check for convergence.
        RunningJob prop_result = algorithm_propagation.getFinalStatus();
        changed_labels = MRConsoleReader.getRecordNum(prop_result, "PROPAGATION", "CHANGED_LABELS");
        if(changed_labels < 0)
          // The last round of iteratioin finished.
          changed_labels = 0;
        System.out.println("++++++>Number of changed labels:" + changed_labels);
      }while(changed_labels!=0);
      
      // 3. Summarize the result as VertexSet.
      /**
      System.out.println("++++++>"+dirMgr.getSeqNum()+": VertexSetSummarize");
      GraphAlgorithm summarize = new VertexSetSummarize();
      summarize.setConf(context);
      Graph src = new Graph(Graph.defaultGraph());
      src.setPath(last_propagation_path);
      Graph dest = new Graph(Graph.defaultGraph());
      tmpDir = dirMgr.getTempDir();
      dest.setPath(tmpDir);
      summarize.setSource(src);
      summarize.setDestination(dest);
      summarize.setMapperNum(getMapperNum());
      summarize.setReducerNum(getReducerNum());
      summarize.execute();
      */
      
      // 4. Extract WCC as partitions.
      System.out.println("++++++>"+dirMgr.getSeqNum()+": ExtractPartitions");
      GraphAlgorithm extract = new ExtractPartitions();
      extract.setConf(context);
      Graph src = new Graph(Graph.defaultGraph());
      src.setPath(last_propagation_path);
      Graph dest = new Graph(Graph.defaultGraph());
      dest.setPath(getDestination().getPath());
      extract.setSource(src);
      extract.setDestination(dest);
      extract.setMapperNum(getMapperNum());
      extract.setReducerNum(getReducerNum());
      extract.execute();
      
      // Delete all temporary directories.
      dirMgr.deleteAll();
    } catch (IllegalAccessException e) {
      throw new ProcessorExecutionException(e);
    } catch (IOException e) {
      throw new ProcessorExecutionException(e);
    }
  }

  /**
   * @param args
   */
  public static void main(String[] args) {
    try {
      int res = ToolRunner.run(new TraversalBasedAlgorithm(), args);
      System.exit(res);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
