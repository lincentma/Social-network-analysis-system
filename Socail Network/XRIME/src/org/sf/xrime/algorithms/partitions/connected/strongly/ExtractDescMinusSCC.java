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
 * This class is used to extract Desc(G,v)\SCC(G,v).
 * @author xue
 *
 */
public class ExtractDescMinusSCC extends GraphAlgorithm {
  /**
   * Default constructor.
   */
  public ExtractDescMinusSCC(){
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
        OutputCollector<Text, LabeledAdjBiSetVertex> output, Reporter reporter)
        throws IOException {
      // Desc(G,v)\SCC(G,v) = vertexes with forward label but without backward label.
      if(value.getStringLabel(ConstantLabels.FORWARD_LABEL)!=null &&
         value.getStringLabel(ConstantLabels.BACKWARD_LABEL)==null){
        for(AdjVertexEdge edge : value.getBackwardVertexes()){
          LabeledAdjBiSetVertex notifier = new LabeledAdjBiSetVertex();
          notifier.setId(edge.getOpposite());
          notifier.setStringLabel(ConstantLabels.DESCENDANT_TO_BE_RETAINED, key.toString());
          output.collect(new Text(edge.getOpposite()), notifier);
        }
        
        for(AdjVertexEdge edge : value.getForwardVertexes()){
          LabeledAdjBiSetVertex notifier = new LabeledAdjBiSetVertex();
          notifier.setId(edge.getOpposite());
          notifier.setStringLabel(ConstantLabels.ANCESTOR_TO_BE_RETAINED, key.toString());
          output.collect(new Text(edge.getOpposite()), notifier);
        }
        
        // We need to output the vertex with all labels removed, since we need to
        // start the next iteration from vertexes with no labels.
        value.clearLabels();
        value.clearBackwardVertex();
        value.clearForwardVertex();
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
        OutputCollector<Text, LabeledAdjBiSetVertex> output, Reporter reporter)
        throws IOException {
      LabeledAdjBiSetVertex the_vertex = null;
      HashSet<String> ancestor_to_be_retained = new HashSet<String>();
      HashSet<String> descendant_to_be_retained = new HashSet<String>();
      while(values.hasNext()){
        LabeledAdjBiSetVertex curr = values.next();
        String ancestor = curr.getStringLabel(ConstantLabels.ANCESTOR_TO_BE_RETAINED);
        String descendant = curr.getStringLabel(ConstantLabels.DESCENDANT_TO_BE_RETAINED);
        if(ancestor!=null){
          ancestor_to_be_retained.add(ancestor);
        }else if(descendant!=null){
          descendant_to_be_retained.add(descendant);
        }else{
          // The Desc\SCC vertex.
          the_vertex = new LabeledAdjBiSetVertex();
          the_vertex.setId(key.toString());
        }
      }
      
      // Fix the vertex up.
      if(the_vertex!=null){
        for(String id : ancestor_to_be_retained){
          the_vertex.addBackwardVertex(new AdjVertexEdge(id));
        }
        for(String id : descendant_to_be_retained){
          the_vertex.addForwardVertex(new AdjVertexEdge(id));
        }
        output.collect(key, the_vertex);
      }
    }
  }
  
  @Override
  public void execute() throws ProcessorExecutionException {
    JobConf conf = new JobConf(context, ExtractDescMinusSCC.class);
    conf.setJobName("ExtractDescMinusSCC");
 
    // the keys are vertex identifiers (strings)
    conf.setOutputKeyClass(Text.class);
    // the values are vertexes (Writable)
    conf.setOutputValueClass(LabeledAdjBiSetVertex.class);
    conf.setMapperClass(MapClass.class);        
    // No combiners.
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
    } catch (IOException e) {
      throw new ProcessorExecutionException(e);
    }
  }
}
