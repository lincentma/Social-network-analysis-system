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
import org.sf.xrime.model.edge.Edge;
import org.sf.xrime.model.edge.EdgeSet;


/**
 * This algorithm is used to summarize the partition on the edge set. It is for human reading purpose.
 * @author xue
 */
public class EdgeSetSummarize extends GraphAlgorithm {
  /**
   * Default constructor.
   */
  public EdgeSetSummarize(){
    super();
  }
  
  /**
   * Mapper. Take the label (v1) as the key (k2), take the edge (k1) as the value (v2).
   * @author xue
   */
  public static class MapClass extends GraphAlgorithmMapReduceBase implements
    Mapper<Text, EdgeSet, Text, EdgeSet>{

    @Override
    public void map(Text key, EdgeSet value,
        OutputCollector<Text, EdgeSet> output, Reporter reporter)
        throws IOException {
      // Reconstruct the edge.
      int index_of_sharp = key.toString().indexOf(ConstantLabels.NON_ID_CHAR);
      String from = key.toString().substring(0, index_of_sharp);
      String to = key.toString().substring(index_of_sharp+1, key.toString().length());
      Edge edge_in_component = new Edge(from,to);
      
      EdgeSet result = new EdgeSet();
      result.addEdge(edge_in_component);
      
      // Get the label edge.
      Edge label_edge = (Edge) value.getEdges().toArray()[0];
      // Emit.
      output.collect(new Text(label_edge.toString()), result);
    }
  }
  
  /**
   * Reducer. Collect all edges belonging to each component.
   * @author xue
   */
  public static class ReduceClass extends GraphAlgorithmMapReduceBase implements
    Reducer<Text, EdgeSet, Text, EdgeSet>{
    @Override
    public void reduce(Text key, Iterator<EdgeSet> values,
        OutputCollector<Text, EdgeSet> output, Reporter reporter)
        throws IOException {
      EdgeSet result = new EdgeSet();
      while(values.hasNext()){
        EdgeSet curr_set = values.next();
        // No need to do deep clone.
        result.addEdges(curr_set.getEdges());
      }
      output.collect(key, result);
    }
  }

  @Override
  public void execute() throws ProcessorExecutionException {
    JobConf conf = new JobConf(context, EdgeSetSummarize.class);
    conf.setJobName("EdgeSetSummarize");
    
    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(EdgeSet.class);
    conf.setMapperClass(MapClass.class);        
    conf.setCombinerClass(ReduceClass.class);
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
