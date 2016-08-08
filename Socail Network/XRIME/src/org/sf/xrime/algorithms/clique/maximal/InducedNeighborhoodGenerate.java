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
package org.sf.xrime.algorithms.clique.maximal;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;

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
import org.sf.xrime.model.edge.EdgeSet;
import org.sf.xrime.model.vertex.LabeledAdjSetVertex;


/**
 * This algorithm is used to generate induced neighborhood from neighborhood.
 * @author xue
 */
public class InducedNeighborhoodGenerate extends GraphAlgorithm {
  /**
   * Default constructor.
   */
  public InducedNeighborhoodGenerate(){
    super();
  }
  
  /**
   * Mapper. Tell each neighbor the neighborhood of myself.
   * @author xue
   */
  public static class MapClass extends GraphAlgorithmMapReduceBase implements
    Mapper<Text, LabeledAdjSetVertex, Text, LabeledAdjSetVertex>{

    @Override
    public void map(Text key, LabeledAdjSetVertex value,
        OutputCollector<Text, LabeledAdjSetVertex> output, Reporter reporter)
        throws IOException {
      // Tell each neighbor my neighborhood.
      for(AdjVertexEdge oppo : value.getOpposites()){
        // Generate a notifier.
        LabeledAdjSetVertex notifier = new LabeledAdjSetVertex();
        notifier.setId(oppo.getOpposite());
        notifier.setLabel(ConstantLabels.NEIGHBOR_NEIGHBORS, value);
        // Notify the neighbor.
        output.collect(new Text(oppo.getOpposite()), notifier);
      }
      
      // Make myself shown in reducer.
      output.collect(key, value);
    }
  }
  
  /**
   * Reducer. Summarize the neighborhood of all my neighbors and generate my induced
   * neighborhood. And, filter out some vertexes from calculating the maximal clique.
   * 
   * @author xue
   */
  public static class ReduceClass extends GraphAlgorithmMapReduceBase implements
    Reducer<Text, LabeledAdjSetVertex, Text, LabeledAdjSetVertex>{

    @Override
    public void reduce(Text key, Iterator<LabeledAdjSetVertex> values,
        OutputCollector<Text, LabeledAdjSetVertex> output, Reporter reporter)
        throws IOException {
      // Ids of neighbors of this vertex.
      HashSet<String> my_neighbors = new HashSet<String>();
      // Used to record the mapping from id of my neighbor, to the id set of the neighbor and its
      // neighbors.
      HashMap<String, HashSet<String>> neighbor_nhs = new HashMap<String, HashSet<String>>();
      // Indicate whether we have already checked the neighor's (neighborhood + neighbor itself) for
      // containment.
      HashMap<String, String> neighbor_nhs_checked = new HashMap<String, String>();
      // Edge set which represents the induced neighborhood of this vertex.
      HashSet<Edge> induced_neighborhood = new HashSet<Edge>();
      
      while(values.hasNext()){
        LabeledAdjSetVertex curr_vertex = values.next();
        if(curr_vertex.getLabel(ConstantLabels.NEIGHBOR_NEIGHBORS)==null){
          // This is myself. Collect the ids of my neighbors.
          for(AdjVertexEdge oppo : curr_vertex.getOpposites()){
            my_neighbors.add(oppo.getOpposite());
          }
        }else{
          // Get the neighborhood of this neighbor.
          LabeledAdjSetVertex neighbor_nh = (LabeledAdjSetVertex) 
                      curr_vertex.getLabel(ConstantLabels.NEIGHBOR_NEIGHBORS);
          // Generate a set of vertex ids which includes the neighbor and all its neighbors.
          HashSet<String> temp_idset = new HashSet<String>();
          // Add the neighbor's id first.
          temp_idset.add(neighbor_nh.getId());
          // For each neighbor of the neighbor.
          for(AdjVertexEdge oppo : neighbor_nh.getOpposites()){
            // Then add all ids of the neighbors of the neighbor.
            temp_idset.add(oppo.getOpposite());
          }
          if(my_neighbors.size()!=0){
            if(temp_idset.size()>my_neighbors.size() &&
               temp_idset.containsAll(my_neighbors)){
              // Already know all my neighbors, and they are all included in one
              // of my neighbor's (neighborhood + the neighbor itself). 
              
              // Need to distinguish "<" and "<="
              if(temp_idset.size()>(my_neighbors.size()+1)){
                // I (this vertex) should not participate in the following calculation.
                return;
              }else if(neighbor_nh.getId().compareTo(key.toString())<=0){
                // When the set is the same, filter out the vertex which has lexically larger id.
                return;
              }
            }
            // Indicate that this neighbor's (neighborhood + id) has already been checked.
            neighbor_nhs_checked.put(neighbor_nh.getId(), "checked");
          }
          // Record the mapping from the id of this vertex's neighbor to temp_idset.
          neighbor_nhs.put(neighbor_nh.getId(), temp_idset);
        }
      }
      
      // Determine whether this vertex (I, me, my) should be removed from the following maximal
      // clique calculation. If the my neighbors are all contained in one of my neighbor's 
      // corresponding set, I should not paticipate in the calculation of maximal cliques.
      // Speculative check has already been done in the while loop above, but we still need
      // to check here.
      for(String neighbor_id : neighbor_nhs.keySet()){
        // Whether have we already check this neighbor's (neighborhood + id).
        if(neighbor_nhs_checked.get(neighbor_id)!=null) continue;
        // Get this neighbor's (neighborhood + id).
        HashSet<String> temp_set = neighbor_nhs.get(neighbor_id);
        if(temp_set.size()>my_neighbors.size()&&
            temp_set.containsAll(my_neighbors)){
          // Need to distinguish "<" and "<="
          if(temp_set.size()>(my_neighbors.size()+1)){
            // This vertex should not participate in the following calculation.
            return;
          }else if(neighbor_id.compareTo(key.toString())<=0){
            // When the set is the same, filter out the vertex which has lexically larger id.
            return;
          }
        }
      }
      
      // Find out the potential edges in the induced neighborhood of this vertex, which
      // come from this neighbor and its neighbors.
      for(String neighbor_id : neighbor_nhs.keySet()){
        // A neighbor of mine and its neighbors.
        HashSet<String> temp_set = neighbor_nhs.get(neighbor_id);
        temp_set.remove(neighbor_id);
        for(String neighbor_nb: temp_set){
          if(neighbor_nb.compareTo(key.toString())==0 ||  // (neighbor_id, I) does not belong to my induced
                                                          // neighborhood. 
             (!my_neighbors.contains(neighbor_nb))){      // neighbor_nb does not belong to my neighborhood.
            // Do nothing.
          }else{
            if(neighbor_id.compareTo(neighbor_nb)<=0){
              // One edge may be added twice, since neighbor_id and neighbor_nb are mutual neighbors.
              // The set can deal with this.
              Edge new_edge = new Edge(neighbor_id, neighbor_nb);
              induced_neighborhood.add(new_edge);
            }else{
              Edge new_edge = new Edge(neighbor_nb, neighbor_id);
              induced_neighborhood.add(new_edge);
            }
          }
        }
      }
 
      // Generate and emit the final result.
      LabeledAdjSetVertex result = new LabeledAdjSetVertex();
      result.setId(key.toString());
      // Neighbors.
      HashSet<AdjVertexEdge> result_vertex_set = new HashSet<AdjVertexEdge>();
      for(String id : my_neighbors){
        result_vertex_set.add(new AdjVertexEdge(id));
      }
      result.setOpposites(result_vertex_set);
      // Edges in induced neighborhood.
      EdgeSet result_edge_set = new EdgeSet();
      result_edge_set.setEdges(induced_neighborhood);
      result.setLabel(ConstantLabels.INDUCED_NEIGHBORHOOD, result_edge_set);
      output.collect(key, result);
    }
  }

  @Override
  public void execute() throws ProcessorExecutionException {
    JobConf conf = new JobConf(context, InducedNeighborhoodGenerate.class);
    conf.setJobName("InducedNeighborhoodGenerate");
    
    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(LabeledAdjSetVertex.class);
    conf.setMapperClass(MapClass.class);        
    // No combiner is permitted, since the logic of reducer depends on the completeness
    // of information.
    conf.setReducerClass(ReduceClass.class);
    // makes the file format suitable for machine processing.
    conf.setInputFormat(SequenceFileInputFormat.class);
    conf.setOutputFormat(SequenceFileOutputFormat.class);
    // Enable compression.
    conf.setCompressMapOutput(true);
    conf.setMapOutputCompressorClass(GzipCodec.class);
    try {
      FileInputFormat.setInputPaths(conf, getSource().getPath());
      FileOutputFormat.setOutputPath(conf, getDestination().getPath());
    } catch (IllegalAccessException e1) {
      throw new ProcessorExecutionException(e1);
    }
    conf.setNumMapTasks(getMapperNum());
    conf.setNumReduceTasks(getReducerNum());
        
    try {
      this.runningJob = JobClient.runJob(conf);
    } catch (IOException e) {
      throw new ProcessorExecutionException(e);
    }
  }
}
