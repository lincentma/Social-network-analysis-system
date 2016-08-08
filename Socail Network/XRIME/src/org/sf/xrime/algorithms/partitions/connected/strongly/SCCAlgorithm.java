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
package org.sf.xrime.algorithms.partitions.connected.strongly;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.ToolRunner;
import org.sf.xrime.ProcessorExecutionException;
import org.sf.xrime.Transformer;
import org.sf.xrime.algorithms.GraphAlgorithm;
import org.sf.xrime.algorithms.transform.vertex.InAdjVertex2AdjBiSetVertexTransformer;
import org.sf.xrime.algorithms.transform.vertex.Vertex2LabeledTransformer;
import org.sf.xrime.model.Graph;
import org.sf.xrime.model.vertex.LabeledAdjBiSetVertex;
import org.sf.xrime.utils.MRConsoleReader;
import org.sf.xrime.utils.SequenceTempDirMgr;


/**
 * Algorithm to calculate strongly connected components of a graph. We assume the input is the original
 * graph represented with incoming adjacency vertexes lists.
 * @author xue
 */
public class SCCAlgorithm extends GraphAlgorithm {
  /**
   * Used to generate sequential temporary paths.
   */
  private SequenceTempDirMgr _dirMgr;
  /**
   * The current temporary path. This is also used as a flag to indicate that
   * the algorithm should stop. When this is set to null, the algorithm should
   * stop.
   */
  private Path _curr_path;
  /**
   * Used to accumulate scc components.
   */
  private List<Path> _scc_components;
  /**
   * Default constructor is not allowed.
   */
  private SCCAlgorithm(){
    super();
  }
  /**
   * Constructor.
   * @throws ProcessorExecutionException 
   */
  public SCCAlgorithm(Graph src, Graph dest) throws ProcessorExecutionException{
    // Typical initialization.
    this();
    this.setSource(src);
    this.setDestination(dest);
    _scc_components = new LinkedList<Path>();
    
    // Check the paths.
    if(getSource().getPaths()==null||getSource().getPaths().size()==0||
        getDestination().getPaths()==null||getDestination().getPaths().size()==0){
      throw new ProcessorExecutionException("No input and/or output paths specified.");
    }
    
    // The prefix used by temp directories which store intermediate results of each steps.
    String temp_dir_prefix;
    try {
      temp_dir_prefix = getDestination().getPath().getParent().toString()+"/scc_alg1_"+
                          getDestination().getPath().getName()+"_";
      // Set the path.
      _curr_path = getSource().getPath();
    } catch (IllegalAccessException e) {
      throw new ProcessorExecutionException(e);
    }
    
    // Create the temporary directory manager.
    _dirMgr = new SequenceTempDirMgr(temp_dir_prefix, context);
    // Sequence number begins with zero.
    _dirMgr.setSeqNum(0);
  }
  
  
  /**
   * Do the transformation needed at the beginning of this algorithm.
   * @throws ProcessorExecutionException
   */
  private void preTransform() throws ProcessorExecutionException{
    // 0.1 Transform the incoming adjacency vertexes lists into AdjBiSetVertex.
    System.out.println("##########>" + _dirMgr.getSeqNum() + " Transform input to AdjBiSetVertex");
    Transformer transformer = new InAdjVertex2AdjBiSetVertexTransformer();
    transformer.setConf(context);
    transformer.setSrcPath(_curr_path);
    // Generate temporary directory.
    try {
      _curr_path = _dirMgr.getTempDir();
    } catch (IOException e) {
      throw new ProcessorExecutionException(e);
    }
    // And use it as the destination directory.
    transformer.setDestPath(_curr_path);
    transformer.setMapperNum(getMapperNum());
    transformer.setReducerNum(getReducerNum());
    transformer.execute();
    
    // 0.2 Transform to LabeledAdjBiSetVertex.
    System.out.println("##########>" + _dirMgr.getSeqNum() + " Transform to LabeledAdjBiSetVertex");
    Vertex2LabeledTransformer l_transformer = new Vertex2LabeledTransformer();
    l_transformer.setConf(context);
    l_transformer.setSrcPath(_curr_path);
    // Generate temporary directory.
    try {
      _curr_path = _dirMgr.getTempDir();
    } catch (IOException e) {
      throw new ProcessorExecutionException(e);
    }
    // And use it as the destination directory.
    l_transformer.setDestPath(_curr_path);
    l_transformer.setMapperNum(getMapperNum());
    l_transformer.setReducerNum(getReducerNum());
    // Set the class used to add initial labels.
    l_transformer.setLabelAdderClass(null);
    // Don't forget this.
    l_transformer.setOutputValueClass(LabeledAdjBiSetVertex.class);
    l_transformer.execute();
  }
  
  /**
   * Check the size of the graph in term of vertexes number.
   * @throws ProcessorExecutionException
   */
  private void checkSize() throws ProcessorExecutionException {
    // 1. Check the size of input graph.
    System.out.println("##########>" + _dirMgr.getSeqNum() + " Check graph size");
    Graph src = new Graph(Graph.defaultGraph());
    src.setPath(_curr_path);
    Path tmpDir;
    try {
      tmpDir = _dirMgr.getTempDir();
    } catch (IOException e) {
      throw new ProcessorExecutionException(e);
    }
    Graph dest = new Graph(Graph.defaultGraph());
    dest.setPath(tmpDir);
    GraphAlgorithm check_size = new CountSize();
    check_size.setConf(context);
    check_size.setSource(src);
    check_size.setDestination(dest);
    check_size.setMapperNum(getMapperNum());
    check_size.setReducerNum(getReducerNum());
    check_size.execute();
    
    long graph_size = MRConsoleReader.getMapInputRecordNum(check_size.getFinalStatus());
    if(graph_size == 0){
      _curr_path = null;
    }else if(graph_size == 1){
      _scc_components.add(_curr_path);
      _curr_path = null;
    }
  }
  
  /**
   * Forward trim.
   * @param dirMgr
   * @param input_path
   * @param result_paths
   * @return
   * @throws ProcessorExecutionException
   */
  private void trimForward() throws ProcessorExecutionException{
    Graph src;
    Graph dest;
    long trimed_graph_size = -1;
    long remaining_graph_size = -1;
    Path tmpDir;
    // 2.1 Forward trim the graph.
    while(true){
      // 2.1.1 ForwardTrimPartA.
      System.out.println("##########>"+_dirMgr.getSeqNum()+" ForwardTrimPartA");
      GraphAlgorithm forward_trim_a = new ForwardTrimPartA();
      forward_trim_a.setConf(context);
      src = new Graph(Graph.defaultGraph());
      // Use the output directory of last step as the input directory of this step.
      src.setPath(_curr_path);
      dest = new Graph(Graph.defaultGraph());
      // Generate a new temporary directory.
      try {
        tmpDir = _dirMgr.getTempDir();
      } catch (IOException e) {
        throw new ProcessorExecutionException(e);
      }
      dest.setPath(tmpDir);
      forward_trim_a.setSource(src);
      forward_trim_a.setDestination(dest);
      forward_trim_a.setMapperNum(getMapperNum());
      forward_trim_a.setReducerNum(getReducerNum());
      forward_trim_a.execute();
      
      // 2.1.2 Check for forward trimming termination. Condition 1: Nothing to be trimed.
      trimed_graph_size = MRConsoleReader.getMapOutputRecordNum(forward_trim_a.getFinalStatus());
      if(trimed_graph_size==0){
        // Forward trim loop should stop since all vertexes in remaining graph have ancestors.
        break;
      }else{
        // Add this path as special SCC components.
        _scc_components.add(tmpDir);
      }
      
      // 2.1.3 ForwardTrimPartB.
      System.out.println("##########>"+_dirMgr.getSeqNum()+" ForwardTrimPartB");
      GraphAlgorithm forward_trim_b = new ForwardTrimPartB();
      forward_trim_b.setConf(context);
      src = new Graph(Graph.defaultGraph());
      src.setPath(_curr_path);
      dest = new Graph(Graph.defaultGraph());
      try {
        tmpDir = _dirMgr.getTempDir();
      } catch (IOException e) {
        throw new ProcessorExecutionException(e);
      }
      dest.setPath(tmpDir);
      forward_trim_b.setSource(src);
      forward_trim_b.setDestination(dest);
      forward_trim_b.setMapperNum(getMapperNum());
      forward_trim_b.setReducerNum(getReducerNum());
      forward_trim_b.execute();
      
      // 2.1.4 Check for forward trimming termination. Condition 2: Remaining graph is zero.
      remaining_graph_size = MRConsoleReader.getReduceOutputRecordNum(forward_trim_b.getFinalStatus());
      if(remaining_graph_size==0){
        // Forward trim loop, and the whole algorithm should stop since the remaining graph is zero.
        _curr_path = null;
        break;
      }else{
        // Remember this end.
        _curr_path = tmpDir;
      }
    }
  }
  
  /**
   * 
   * @throws ProcessorExecutionException
   */
  private void trimBackward() throws ProcessorExecutionException {
    Graph src;
    Graph dest;
    long trimed_graph_size = -1;
    long remaining_graph_size = -1;
    Path tmpDir;
    // 2.2 Backward trim the graph.
    while(true){
      // 2.2.1 BackwardTrimPartA.
      System.out.println("##########>"+_dirMgr.getSeqNum()+" BackwardTrimPartA");
      GraphAlgorithm backward_trim_a = new BackwardTrimPartA();
      backward_trim_a.setConf(context);
      src = new Graph(Graph.defaultGraph());
      // Pick the right input path.
      src.setPath(_curr_path);
      dest = new Graph(Graph.defaultGraph());
      // Generate a new temporary directory.
      try {
        tmpDir = _dirMgr.getTempDir();
      } catch (IOException e1) {
        throw new ProcessorExecutionException(e1);
      }
      dest.setPath(tmpDir);
      backward_trim_a.setSource(src);
      backward_trim_a.setDestination(dest);
      backward_trim_a.setMapperNum(getMapperNum());
      backward_trim_a.setReducerNum(getReducerNum());
      backward_trim_a.execute();
      
      // 2.2.2 Check for backward trimming termination. Condition 1: Nothing to be trimed.
      trimed_graph_size = MRConsoleReader.getMapOutputRecordNum(backward_trim_a.getFinalStatus());
      if(trimed_graph_size==0){
        // Backward trim loop should stop since all vertexes in remaining graph have descendants.
        break;
      }else{
        // Add this path as special SCC components.
        _scc_components.add(tmpDir);
      }
      
      // 2.2.3 BackwardTrimPartB.
      System.out.println("##########>"+_dirMgr.getSeqNum()+" BackwardTrimPartB");
      GraphAlgorithm backward_trim_b = new BackwardTrimPartB();
      backward_trim_b.setConf(context);
      src = new Graph(Graph.defaultGraph());
      src.setPath(_curr_path);
      dest = new Graph(Graph.defaultGraph());
      try {
        tmpDir = _dirMgr.getTempDir();
      } catch (IOException e) {
        throw new ProcessorExecutionException(e);
      }
      dest.setPath(tmpDir);
      backward_trim_b.setSource(src);
      backward_trim_b.setDestination(dest);
      backward_trim_b.setMapperNum(getMapperNum());
      backward_trim_b.setReducerNum(getReducerNum());
      backward_trim_b.execute();
      
      // 2.2.4 Check for backward trimming termination. Condition 2: Remaining graph is zero.
      remaining_graph_size = MRConsoleReader.getReduceOutputRecordNum(backward_trim_b.getFinalStatus());
      if(remaining_graph_size==0){
        // Backward trim loop should stop since the remaining graph is zero.
        _curr_path = null;
        break;
      }else{
        // Remember this end.
        _curr_path = tmpDir;
      }
    }
  }
  
  /**
   * Choose a pivot vertex from the graph.
   * @return return the id of the chosen vertex.
   * @throws ProcessorExecutionException
   */
  private String choosePivotVertex() throws ProcessorExecutionException{
    String result = null;
    Graph src;
    Graph dest;
    Path tmpDir;
    
    // 3.1. Choose a pivot vertex.
    System.out.println("##########>"+_dirMgr.getSeqNum()+" Choose the pivot vertex");
    src = new Graph(Graph.defaultGraph());
    src.setPath(_curr_path);
    dest = new Graph(Graph.defaultGraph());
    try {
      tmpDir = _dirMgr.getTempDir();
    } catch (IOException e) {
      throw new ProcessorExecutionException(e);
    }
    dest.setPath(tmpDir);
    
    GraphAlgorithm choose_pivot = new PivotChoose();
    choose_pivot.setConf(context);
    choose_pivot.setSource(src);
    choose_pivot.setDestination(dest);
    choose_pivot.setMapperNum(getMapperNum());
    choose_pivot.setReducerNum(getReducerNum());
    choose_pivot.execute();
    
    // 3.2 Read the vertex.
    try {
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
      result = the_line.substring(PivotChoose.KEY_PIVOT.length()).trim();
      // Close the input and output stream.
      input_stream.close();
      output_stream.close();
      System.out.println("##########> Chosen pivot id = " + result);
    } catch (IOException e) {
      throw new ProcessorExecutionException(e);
    }
    return result;
  }
  
  /**
   * Do labels propagation.
   * @param label
   * @throws ProcessorExecutionException
   */
  private void propagateLabel(String label) throws ProcessorExecutionException {
    Graph src;
    Graph dest;
    long label_num = -1;
    Path tmpDir;
    while(true){
      // 4.1. Label propagation.
      System.out.println("##########>"+_dirMgr.getSeqNum()+" Propagate the label : (" + label +") from " +
          _curr_path.toString());
      src = new Graph(Graph.defaultGraph());
      src.setPath(_curr_path);
      try {
        tmpDir = _dirMgr.getTempDir();
      } catch (IOException e) {
        throw new ProcessorExecutionException(e);
      }
      dest = new Graph(Graph.defaultGraph());
      dest.setPath(tmpDir);
      
      GraphAlgorithm propagator = new LabelPropagation();
      propagator.setConf(context);
      propagator.setSource(src);
      propagator.setDestination(dest);
      propagator.setMapperNum(getMapperNum());
      propagator.setReducerNum(getReducerNum());
      // Set the label to be propagated. This is important.
      propagator.setParameter(ConstantLabels.SCC_LABEL, label);
      propagator.execute();
      
      // Update progress.
      _curr_path = tmpDir;
      
      // 4.2. Convergence test.
      System.out.println("##########>"+_dirMgr.getSeqNum()+" Test label propagation convergence");
      src = new Graph(Graph.defaultGraph());
      src.setPath(_curr_path);
      try {
        tmpDir = _dirMgr.getTempDir();
      } catch (IOException e) {
        throw new ProcessorExecutionException(e);
      }
      dest = new Graph(Graph.defaultGraph());
      dest.setPath(tmpDir);
      
      GraphAlgorithm conv_tester = new PropagationConvergenceTest();
      conv_tester.setConf(context);
      conv_tester.setSource(src);
      conv_tester.setDestination(dest);
      conv_tester.setMapperNum(getMapperNum());
      conv_tester.setReducerNum(getReducerNum());
      conv_tester.execute();
      
      // 4.3. Check test result.
      long found_label_num = MRConsoleReader.getMapOutputRecordNum(conv_tester.getFinalStatus());
      System.out.println("##########> label number = " + found_label_num);
      if(found_label_num == label_num){
        // Converged.
        break;
      }else{
        label_num = found_label_num;
      }
    }
  }
  
  /**
   * Extract a SCC from propagation result.
   * @throws ProcessorExecutionException
   */
  private void extractSCC() throws ProcessorExecutionException {
    Graph src;
    Graph dest;
    Path tmpDir;
    
    // 5.1. Extract SCC.
    System.out.println("##########>"+_dirMgr.getSeqNum()+" Extract a SCC");
    src = new Graph(Graph.defaultGraph());
    src.setPath(_curr_path);
    try {
      tmpDir = _dirMgr.getTempDir();
    } catch (IOException e) {
      throw new ProcessorExecutionException(e);
    }
    dest = new Graph(Graph.defaultGraph());
    dest.setPath(tmpDir);
    
    GraphAlgorithm extractor = new ExtractSCC();
    extractor.setConf(context);
    extractor.setSource(src);
    extractor.setDestination(dest);
    extractor.setMapperNum(getMapperNum());
    extractor.setReducerNum(getReducerNum());
    extractor.execute();
    
    // Add the path to the components list.
    _scc_components.add(tmpDir);
  }

  /**
   * Extract Pred(G,v)\SCC(G,v).
   * @return the path to the extracted Pred\SCC.
   * @throws ProcessorExecutionException
   */
  private Path extractPredMinusSCC() throws ProcessorExecutionException {
    Graph src;
    Graph dest;
    Path tmpDir;
    
    // 5.2. Extract Pred\SCC.
    System.out.println("##########>"+_dirMgr.getSeqNum()+" Extract Pred\\SCC");
    src = new Graph(Graph.defaultGraph());
    src.setPath(_curr_path);
    dest = new Graph(Graph.defaultGraph());
    try {
      tmpDir = _dirMgr.getTempDir();
    } catch (IOException e) {
      throw new ProcessorExecutionException(e);
    }
    dest.setPath(tmpDir);
    
    GraphAlgorithm extractor = new ExtractPredMinusSCC();
    extractor.setConf(context);
    extractor.setSource(src);
    extractor.setDestination(dest);
    extractor.setMapperNum(getMapperNum());
    extractor.setReducerNum(getReducerNum());
    extractor.execute();
    
    return tmpDir;
  }
  
  /**
   * Extract Desc(G,v)\SCC(G,v).
   * @return the path to Desc\SCC.
   * @throws ProcessorExecutionException
   */
  private Path extractDescMinusSCC() throws ProcessorExecutionException {
    Graph src;
    Graph dest;
    Path tmpDir;
    
    // 5.3. Extract Desc\SCC.
    System.out.println("##########>"+_dirMgr.getSeqNum()+" Extract Desc\\SCC");
    src = new Graph(Graph.defaultGraph());
    src.setPath(_curr_path);
    dest = new Graph(Graph.defaultGraph());
    try {
      tmpDir = _dirMgr.getTempDir();
    } catch (IOException e) {
      throw new ProcessorExecutionException(e);
    }
    dest.setPath(tmpDir);
    
    GraphAlgorithm extractor = new ExtractDescMinusSCC();
    extractor.setConf(context);
    extractor.setSource(src);
    extractor.setDestination(dest);
    extractor.setMapperNum(getMapperNum());
    extractor.setReducerNum(getReducerNum());
    extractor.execute();
    
    return tmpDir;
  }
  
  /**
   * Extract Rem(G,v).
   * @return path to Rem.
   * @throws ProcessorExecutionException
   */
  private Path extractRemains() throws ProcessorExecutionException {
    Graph src;
    Graph dest;
    Path tmpDir;
    
    // 5.4. Extract Rem.
    System.out.println("##########>"+_dirMgr.getSeqNum()+" Extract Rem");
    src = new Graph(Graph.defaultGraph());
    src.setPath(_curr_path);
    dest = new Graph(Graph.defaultGraph());
    try {
      tmpDir = _dirMgr.getTempDir();
    } catch (IOException e) {
      throw new ProcessorExecutionException(e);
    }
    dest.setPath(tmpDir);
    
    GraphAlgorithm extractor = new ExtractRemains();
    extractor.setConf(context);
    extractor.setSource(src);
    extractor.setDestination(dest);
    extractor.setMapperNum(getMapperNum());
    extractor.setReducerNum(getReducerNum());
    extractor.execute();
    
    return tmpDir;
  }
  
  /**
   * The recursive part of this algorithm.
   * @throws ProcessorExecutionException
   */
  private void recursiveCore() throws ProcessorExecutionException {
    //1. Check size;
    checkSize();
    if(_curr_path==null){
      return;
    }
    
    //2.1. Trim in forward direction.
    trimForward();
    if(_curr_path==null){
      return;
    }
    
    //2.2. Trim in backward direction.
    trimBackward();
    if(_curr_path==null){
      return;
    }
    
    //3. Choose the pivot vertex.
    String pivot_id = choosePivotVertex();
    
    //4. Propagate labels.
    propagateLabel(pivot_id);
    
    //5.1. Extract SCC.
    extractSCC();
    
    //5.2-5.4. Extract Pred\SCC, Desc\SCC, Rem.
    Path pred_scc = extractPredMinusSCC();
    Path desc_scc = extractDescMinusSCC();
    Path rem = extractRemains();
    
    //6. Recursive invoke.
    //6.1.
    _curr_path = pred_scc;
    recursiveCore();
    //6.2.
    _curr_path = desc_scc;
    recursiveCore();
    //6.3.
    _curr_path = rem;
    recursiveCore();
  }
  
  /**
   * Used to summarize the results.
   * @param paths
   * @throws ProcessorExecutionException
   */
  private void extractComponents() throws ProcessorExecutionException{
    Graph src;
    Graph dest;
    
    System.out.println("##########>"+_dirMgr.getSeqNum()+" Extract the final partitions");
    src = new Graph(Graph.defaultGraph());
    src.setPaths(_scc_components);
    dest = getDestination();
    
    GraphAlgorithm extractor = new ExtractPartitions();
    extractor.setConf(context);
    extractor.setSource(src);
    extractor.setDestination(dest);
    extractor.setMapperNum(getMapperNum());
    //extractor.setReducerNum(getReducerNum());
    extractor.setReducerNum(1);
    extractor.execute();
  }

  @Override
  public void execute() throws ProcessorExecutionException {
    //0. Transform.
    preTransform();
    
    recursiveCore();
    
    if(_scc_components.size()>0){
      extractComponents();
    }
    
    // Delete all temporary directories.
    _dirMgr.deleteAll();
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
  
  /**
   * @param args
   */
  public static void main(String[] args) {
    try {
      int res = ToolRunner.run(new SCCAlgorithm(), args);
      System.exit(res);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
