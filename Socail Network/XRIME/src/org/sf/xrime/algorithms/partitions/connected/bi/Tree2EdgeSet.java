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

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.TreeSet;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.sf.xrime.ProcessorExecutionException;
import org.sf.xrime.algorithms.GraphAlgorithm;
import org.sf.xrime.algorithms.utils.GraphAlgorithmMapReduceBase;
import org.sf.xrime.model.edge.AdjVertexEdge;
import org.sf.xrime.model.edge.Edge;
import org.sf.xrime.model.edge.EdgeComparator;
import org.sf.xrime.model.edge.EdgeSet;
import org.sf.xrime.model.path.PathAsVertexesList;
import org.sf.xrime.model.vertex.LabeledAdjSetVertex;
import org.sf.xrime.model.vertex.Vertex;


/**
 * This algorithm is used to transform the generated spanning tree into the first set
 * of edge sets, which will be treated as relationships over edges. Then we will start
 * to calculate the transitive closure of these relationships and the final result will
 * be the BCCs represented as edge sets.
 * 
 * NOTE: an edge here does not necessarily have the same direction as in the original
 * graph.
 * @author xue
 */
public class Tree2EdgeSet extends GraphAlgorithm {
  /**
   * Default constructor.
   */
  public Tree2EdgeSet(){
    super();
  }
  /**
   * Find the out-tree edges, which will be combined with in-tree edges to form the
   * original edge sets. Map is executed on each vertex, which will have a path to
   * the root of the spanning tree. Edges in this path is in-tree edges, and the other
   * incidental edges of this vertex are out-tree edges. This mapper will find each
   * out-tree edge attached to this vertex, emit the edge as key, and emit the 
   * path to root as value.
   * 
   * NOTE: each vertex of the graph will only appear at a single position in the
   * spanning tree. 
   *
   * @author xue
   */
  public static class MapClass extends GraphAlgorithmMapReduceBase implements
    Mapper<Text, LabeledAdjSetVertex, Text, PathAsVertexesList>{
    @Override
    public void map(Text key, LabeledAdjSetVertex value,
        OutputCollector<Text, PathAsVertexesList> output, Reporter reporter)
        throws IOException {
      // Get the path to root as a list of vertexes.
      PathAsVertexesList path_to_root = (PathAsVertexesList) value.getLabel(ConstantLabels.PATH_TO_ROOT);
      List<Vertex> vertexes = path_to_root.getVertexes();
      if(vertexes.size()>1){
        // This vertex is not the root of the spanning tree, emit key/value pairs.
        // The id of the vertex before the last (this vertex itself) in the path to root.
        String last_vertex = vertexes.get(vertexes.size()-2).getId();
        for(AdjVertexEdge oppo : value.getOpposites()){
          if(last_vertex.compareTo(oppo.getOpposite())==0){
            // Skip the in-tree edges.
          }else{
            // Deal with each potential out-tree edges. Use the potential out-tree
            // edge as key. For reducer's good, we choose the smaller id of the 
            // two ends as from, and the other as to.
            
            // These are only potential out-tree edges. Please read comments in reducer
            // for more details.
            String k2;
            if(key.toString().compareTo(oppo.getOpposite())<=0){
              k2 = key.toString()+ConstantLabels.NON_ID_CHAR+oppo.getOpposite();
            }else{
              k2 = oppo.getOpposite()+ConstantLabels.NON_ID_CHAR+key.toString();
            }
            // Collect the result.
            output.collect(new Text(k2), path_to_root);
          }
        }
      }
    }
  }
  /**
   * Reducer is used to find circles and emit them as edge set.
   * @author xue
   */
  public static class ReduceClass extends GraphAlgorithmMapReduceBase implements
    Reducer<Text, PathAsVertexesList, Text, EdgeSet>{
    @Override
    public void reduce(Text key, Iterator<PathAsVertexesList> values,
        OutputCollector<Text, EdgeSet> output, Reporter reporter)
        throws IOException {
      // Generate the out-tree edge first.
      int index_of_sharp = key.toString().indexOf(ConstantLabels.NON_ID_CHAR);
      String from = key.toString().substring(0, index_of_sharp);
      String to = key.toString().substring(index_of_sharp+1, key.toString().length());
      Edge out_tree_edge = new Edge(from,to);
      
      // There should be only two PathAsVertexesList in values.
      // Get the ids of vertexes in these paths, from leaf to root.
      List<String> path_1 = new LinkedList<String>(); 
      List<String> path_2 = new LinkedList<String>();
      PathAsVertexesList curr_path = null;
      if(values.hasNext()){
        curr_path = values.next();
        List<Vertex> vertexes = curr_path.getVertexes();
        for(int i=vertexes.size()-1; i>=0; i--){
          path_1.add(vertexes.get(i).getId());
        }
        curr_path = null;
      }
      if(values.hasNext()){
        curr_path = values.next();
        List<Vertex> vertexes = curr_path.getVertexes();
        for(int i=vertexes.size()-1; i>=0; i--){
          path_2.add(vertexes.get(i).getId());
        }
        curr_path = null;
      }
      
      if(path_1.size()==0 || path_2.size()==0){
        // It's possible here. k2 emitted by mapper may be an in-tree edge. In this case, the
        // vertex being processed by the mapper must be a none-leaf vertex in the spanning tree,
        // and k2 is an edge to one of its child in the tree. We only skip the edge to its parent
        // in mapper code, and we can not tell this there. But here, we can tell, since the child
        // will never emit an edge to its parent as k2. As a result, in this case, for this k2,
        // only one path will be collected by reducer, which is the path to root of the parent
        // vertex.
        return;       
      }
      
      // The vertex where the two paths intersect. We will find it.
      String pivot = null;
      EdgeSet result_set = new EdgeSet();
      for(int i=0; i<path_1.size()-1; i++){
        // We always take the direction from root to leaf in the spanning tree,
        // take the direction from lexically smaller id to larger id out of the
        // spanning tree. In this way, we could assure that the edge set join
        // operation is correct.
        Edge temp_edge = new Edge(path_1.get(i+1), path_1.get(i));
        result_set.addEdge(temp_edge);
        if(path_2.contains(path_1.get(i+1))){
          // We found the pivot (the first intersecting vertex between these
          // two paths from leaf to root).
          pivot = path_1.get(i+1);
          break;
        }
      }
      if(pivot == null) return; // Shouldn't happen.
      for(int i=0; i<path_2.size()-1; i++){
        Edge temp_edge = new Edge(path_2.get(i+1), path_2.get(i));
        result_set.addEdge(temp_edge);
        if(path_2.get(i+1).compareTo(pivot)==0){
          // Reach at the pivot vertex.
          break;
        }
      }
      
      // Add the out tree edge.
      result_set.addEdge(out_tree_edge);
      
      // Find the lexically smallest edge as k3.
      TreeSet<Edge> order_set = new TreeSet<Edge>(new EdgeComparator());
      order_set.addAll(result_set.getEdges());
      Edge key_edge = order_set.first();
      String k3 = key_edge.getFrom()+ConstantLabels.NON_ID_CHAR+key_edge.getTo();
      // Collect the result.
      output.collect(new Text(k3), result_set);
    }
  }

  @Override
  public void execute() throws ProcessorExecutionException {
    JobConf conf = new JobConf(context, Tree2EdgeSet.class);
    conf.setJobName("Tree2EdgeSet");
    
    conf.setMapOutputKeyClass(Text.class);
    conf.setMapOutputValueClass(PathAsVertexesList.class);
    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(EdgeSet.class);
    conf.setMapperClass(MapClass.class);        
    // No combiner is permitted, since the logic of reducer depends on the completeness
    // of information.
    conf.setReducerClass(ReduceClass.class);
    // makes the file format suitable for machine processing.
    conf.setInputFormat(SequenceFileInputFormat.class);
    conf.setOutputFormat(SequenceFileOutputFormat.class);
    try {
      FileInputFormat.setInputPaths(conf, getSource().getPath());
      FileOutputFormat.setOutputPath(conf, getDestination().getPath());
    } catch (IllegalAccessException e1) {
      throw new ProcessorExecutionException(e1);
    }
    conf.setNumMapTasks(getMapperNum());
    conf.setNumReduceTasks(getReducerNum());
    conf.setCompressMapOutput(true);
    conf.setMapOutputCompressorClass(GzipCodec.class);
        
    try {
      this.runningJob = JobClient.runJob(conf);
    } catch (IOException e) {
      throw new ProcessorExecutionException(e);
    }
  }
}
