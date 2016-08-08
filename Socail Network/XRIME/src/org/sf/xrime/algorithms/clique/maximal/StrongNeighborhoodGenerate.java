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
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

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
import org.sf.xrime.model.vertex.AdjVertex;
import org.sf.xrime.model.vertex.LabeledAdjSetVertex;


/**
 * This algorithm is used to generate strong neighborhood for each vertex in the graph.
 * Take outgoing adjancency lists as the input.
 * @author xue
 */
public class StrongNeighborhoodGenerate extends GraphAlgorithm {
  /**
   * Default constructor.
   */
  public StrongNeighborhoodGenerate(){
    super();
  }
  
  /**
   * Mapper. Used to find bi-directional arcs between vertexes. 
   * @author xue
   */
  public static class MapClass extends GraphAlgorithmMapReduceBase implements
    Mapper<Text, AdjVertex, Text, LabeledAdjSetVertex>{

    @Override
    public void map(Text key, AdjVertex value,
        OutputCollector<Text, LabeledAdjSetVertex> output, Reporter reporter)
        throws IOException {
      List<Edge> tos = value.getEdges();
      if(tos.size()==0) return; // This vertex has no outgoing arc.
      
      // For the remote end of each arc.
      for(int i=0; i<tos.size(); i++){
        String to = tos.get(i).getTo();
        // Notify the neighbor about myself.
        LabeledAdjSetVertex notifier = new LabeledAdjSetVertex();
        notifier.setId(to);
        notifier.setStringLabel(ConstantLabels.POTENTIAL_NEIGHBOR, key.toString());
        output.collect(new Text(to), notifier);
      }
      // Make myself shown in reducer.
      LabeledAdjSetVertex myself = new LabeledAdjSetVertex();
      myself.fromAdjVertexTos(value);
      output.collect(key, myself);
    }
  }
  
  /**
   * Reducer. Settle down the neighborhood of each vertex. 
   * @author xue
   */
  public static class ReduceClass extends GraphAlgorithmMapReduceBase implements
    Reducer<Text, LabeledAdjSetVertex, Text, LabeledAdjSetVertex>{

    @Override
    public void reduce(Text key, Iterator<LabeledAdjSetVertex> values,
        OutputCollector<Text, LabeledAdjSetVertex> output, Reporter reporter)
        throws IOException {
      HashSet<String> potential_neighbors = new HashSet<String>();
      HashSet<AdjVertexEdge> neighbors = new HashSet<AdjVertexEdge>();
      while(values.hasNext()){
        LabeledAdjSetVertex curr_vertex = values.next();
        if(curr_vertex.getStringLabel(ConstantLabels.POTENTIAL_NEIGHBOR)==null){
          // No need to do deep clone here.
          neighbors.addAll(curr_vertex.getOpposites());
        }else{
          // Accumulate potential neighbors.
          potential_neighbors.add(curr_vertex.getStringLabel(ConstantLabels.POTENTIAL_NEIGHBOR));
        }
      }
      for(Iterator<AdjVertexEdge> iterator = neighbors.iterator(); iterator.hasNext();){
        if(potential_neighbors.contains(iterator.next().getOpposite())){
          // This is a neighbor we care about.
        }else{
          // This is not a neighbor we care about, remove it from the set.
          iterator.remove();
        }
      }
      if(neighbors.size()==0) return; // This vertex has no incoming arcs corresponding to outgoing
                                      // arcs.
      
      LabeledAdjSetVertex result = new LabeledAdjSetVertex();
      result.setId(key.toString());
      result.setOpposites(neighbors);
      // Collect this.
      output.collect(key, result);
    }
  }
  
  @Override
  public void execute() throws ProcessorExecutionException {
    JobConf conf = new JobConf(context, StrongNeighborhoodGenerate.class);
    conf.setJobName("StrongNeighborhoodGenerate");
    
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
