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
package org.sf.xrime.algorithms.kcore.undirected;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

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
import org.sf.xrime.model.vertex.AdjSetVertex;


/**
 * This class is recursively used to delete all vertexes, and lines incident with them, of
 * degree less than k. When this process terminates, the remaining graph is the k-core.
 * @author xue
 *
 */
public class ElementRemoval extends GraphAlgorithm {
  /**
   * This is used as the parameter name which specifies the k-core we are interested in.
   */
  public static final String K_OF_CORE = "k_of_core";
  /**
   * Default constructor.
   */
  public ElementRemoval(){
    super();
  }
  
  /**
   * Mapper. Used to remove vertexes and notify the removal of incident lines.
   * @author xue
   */
  public static class MapClass extends GraphAlgorithmMapReduceBase implements
    Mapper<Text, AdjSetVertex, Text, AdjSetVertex>{

    @Override
    public void map(Text key, AdjSetVertex value,
        OutputCollector<Text, AdjSetVertex> output, Reporter reporter)
        throws IOException {
      // Determine the k of k-core.
      int k_of_core = Integer.parseInt(context.getParameter(K_OF_CORE));
      int opps_num = value.getOpposites()==null ? 0 : value.getOpposites().size();
      if(opps_num < k_of_core){
        // The vertex has opps_num neighbours, but the degree is less than k.
        // Remove it and lines incident with it. By not emitting this vertex,
        // we remove it. By sending notifications to its adjacent vertexes, we
        // remove its incident lines.
        
        // Use minimal overhead to do the notification.
        AdjSetVertex notifier = new AdjSetVertex();
        notifier.setId(key.toString());
        for(AdjVertexEdge opposite : value.getOpposites()){
          output.collect(new Text(opposite.getOpposite()), notifier);
        }
      }else{
        // Emit this vertex without changing it.
        output.collect(key, value);
      }
    }
  }
  
  /**
   * Reducer. Used to remove incident lines linking to removed vertexes.
   * @author xue
   */
  public static class ReduceClass extends GraphAlgorithmMapReduceBase implements
    Reducer<Text, AdjSetVertex, Text, AdjSetVertex>{

    @Override
    public void reduce(Text key, Iterator<AdjSetVertex> values,
        OutputCollector<Text, AdjSetVertex> output, Reporter reporter)
        throws IOException {
      AdjSetVertex this_vertex = null;
      Set<String> opps_to_be_removed = new HashSet<String>();
      while(values.hasNext()){
        AdjSetVertex curr = values.next();
        if((curr.getId().compareTo(key.toString())!=0)&& // Maybe unnecessary.
           (curr.getOpposites()==null||curr.getOpposites().size()==0)){
          // opposites to be removed.
          opps_to_be_removed.add(curr.getId());
        }else{
          // The vertex to be processed. This statement should be executed once only.
          this_vertex = new AdjSetVertex(curr);
        }
      }
      if(this_vertex!=null){
        // Remove lines.
        for(String opp : opps_to_be_removed){
          this_vertex.getOpposites().remove(new AdjVertexEdge(opp));
        }
        output.collect(key, this_vertex);
      }else{
        // Do nothing, since this vertex's degree is less than k too, and
        // it has already been removed by mapper.
      }
    }
  }
  
  @Override
  public void execute() throws ProcessorExecutionException {
    // Use the graph algorithm context to create a job configuration object.
    JobConf conf = new JobConf(context, ElementRemoval.class);
    conf.setJobName("ElementRemoval");
 
    // the keys are vertex identifiers (strings)
    conf.setOutputKeyClass(Text.class);
    // the values are vertexes (Writable)
    conf.setOutputValueClass(AdjSetVertex.class);
    conf.setMapperClass(MapClass.class);        
    // Combiner is permitted! But we don't use for now.
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
