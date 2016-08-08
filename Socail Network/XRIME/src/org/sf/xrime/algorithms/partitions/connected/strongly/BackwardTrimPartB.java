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

import java.io.IOException;
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
import org.sf.xrime.model.vertex.LabeledAdjBiSetVertex;


/**
 * The backward trim is to trim vertexes which have no descendants. This class is used to
 * do the real trim.
 * @author xue
 *
 */
public class BackwardTrimPartB extends GraphAlgorithm {
  /**
   * Default constructor.
   */
  public BackwardTrimPartB(){
    super();
  }
  /**
   * Mapper.
   * @author xue
   */
  public static class MapClass extends GraphAlgorithmMapReduceBase implements
    Mapper<Text, LabeledAdjBiSetVertex, Text, LabeledAdjBiSetVertex>{

    @Override
    public void map(Text key, LabeledAdjBiSetVertex value,
        OutputCollector<Text, LabeledAdjBiSetVertex> output,
        Reporter reporter) throws IOException {
      if(value.getForwardVertexes()==null||value.getForwardVertexes().size()==0){
        // This vertex does not have descendants. Trim it and all its edges. So we need
        // to notify all its immediate ancestors.
        for(AdjVertexEdge ancestor : value.getBackwardVertexes()){
          LabeledAdjBiSetVertex notifier = new LabeledAdjBiSetVertex();
          notifier.setId(ancestor.getOpposite());
          notifier.setStringLabel(ConstantLabels.DESCENDANT_TO_BE_REMOVED, value.getId());
          // Tell the ancestor that this vertex is to be removed.
          output.collect(new Text(ancestor.getOpposite()), notifier);
        }
      }else{
        // Normal vertex, emit it.
        output.collect(key, value);
      }
    }
    
  }
  /**
   * Reducer.
   * @author xue
   */
  public static class ReduceClass extends GraphAlgorithmMapReduceBase implements
    Reducer<Text, LabeledAdjBiSetVertex, Text, LabeledAdjBiSetVertex>{

    @Override
    public void reduce(Text key, Iterator<LabeledAdjBiSetVertex> values,
        OutputCollector<Text, LabeledAdjBiSetVertex> output,
        Reporter reporter) throws IOException {
      // The descendants to be removed.
      HashSet<String> descendants_to_be_removed = new HashSet<String>();
      LabeledAdjBiSetVertex ancestor = null;
      while(values.hasNext()){
        LabeledAdjBiSetVertex curr_vertex = values.next();
        if(curr_vertex.getStringLabel(ConstantLabels.DESCENDANT_TO_BE_REMOVED)==null){
          // Clone this vertex for later use. NOT deep clone.
          ancestor = new LabeledAdjBiSetVertex(curr_vertex);
        }else{
          // This vertex has some descendants to be removed.
          descendants_to_be_removed.add(
              curr_vertex.getStringLabel(ConstantLabels.DESCENDANT_TO_BE_REMOVED));
        }
      }
      if(ancestor!=null){
        for(String descendant : descendants_to_be_removed){
          // Remove edges to descendants to be removed.
          ancestor.getForwardVertexes().remove(new AdjVertexEdge(descendant));
        }
        output.collect(key, ancestor);
      }
    }
  }

  @Override
  public void execute() throws ProcessorExecutionException {
    JobConf conf = new JobConf(context, BackwardTrimPartB.class);
    conf.setJobName("BackwardTrimPartB");
    
    // the keys are vertex identifiers (strings)
    conf.setOutputKeyClass(Text.class);
    // the values are vertexes (Writable)
    conf.setOutputValueClass(LabeledAdjBiSetVertex.class);
    conf.setMapperClass(MapClass.class);        
    // No combiner is permitted.
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
    conf.setMapOutputCompressorClass(GzipCodec.class);
    conf.setCompressMapOutput(true);
        
    try {
      this.runningJob = JobClient.runJob(conf);
    } catch (IOException e){
      throw new ProcessorExecutionException(e);
    }
  }
}
