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
import java.util.TreeSet;

import org.apache.hadoop.fs.Path;
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
import org.sf.xrime.model.edge.EdgeComparator;
import org.sf.xrime.model.edge.EdgeSet;


/**
 * Expand higher layer edge sets to lower layer, i.e., propagate label (lexically smallest
 * edge) of a higher layer edge set to bigger edge sets in lower layer.
 * @author xue
 */
public class EdgeSetExpand extends GraphAlgorithm {
  /**
   * Default constructor.
   */
  public EdgeSetExpand(){
    super();
  }
  /**
   * Emit <k1,v1> as <k2,v2>. The input come from both lower layer and upper layer.
   * The input from lower layer is in the form <upper_layer_label, lower_layer_edge_set>.
   * The input from upper layer is in the form <upper_layer_label, final_label_as_set>.
   * @author xue
   */
  public static class MapClass extends GraphAlgorithmMapReduceBase implements
    Mapper<Text, EdgeSet, Text, EdgeSet>{

    @Override
    public void map(Text key, EdgeSet value,
        OutputCollector<Text, EdgeSet> output, Reporter reporter)
        throws IOException {
      output.collect(key, value);
    }
  }
  
  /**
   * Propagate the label to lower layer. <k3,v3> will be in the form
   * <lower_layer_label/edge, final_label_as_set>.
   * @author xue
   */
  public static class ReduceClass extends GraphAlgorithmMapReduceBase implements
    Reducer<Text, EdgeSet, Text, EdgeSet>{
    @Override
    public void reduce(Text key, Iterator<EdgeSet> values,
        OutputCollector<Text, EdgeSet> output, Reporter reporter)
        throws IOException {
      // Collect all edges from lower and upper layers. The label is also in the set,
      // and it's the lexically smallest one.
      TreeSet<Edge> edges = new TreeSet<Edge>(new EdgeComparator());
      while(values.hasNext()){
        EdgeSet curr_set = values.next();
        edges.addAll(curr_set.getEdges());
      }
      EdgeSet label_set = new EdgeSet();
      label_set.addEdge(edges.first());
      
      for(Edge edge : edges){
        String k3 = edge.getFrom()+ConstantLabels.NON_ID_CHAR+edge.getTo();
        output.collect(new Text(k3), label_set);
      }
    }
  }

  @Override
  public void execute() throws ProcessorExecutionException {
    JobConf conf = new JobConf(context, EdgeSetExpand.class);
    conf.setJobName("EdgeSetExpand");
    
    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(EdgeSet.class);
    conf.setMapperClass(MapClass.class);        
    // No combiner is permitted. Or we may get wrong with labeling.
    conf.setReducerClass(ReduceClass.class);
    // makes the file format suitable for machine processing.
    conf.setInputFormat(SequenceFileInputFormat.class);
    conf.setOutputFormat(SequenceFileOutputFormat.class);
    // Two input pathes (link to lower and upper layers), one output pathes.
    try {
      Path[] input_paths = new Path[2];
      FileInputFormat.setInputPaths(conf, getSource().getPaths().toArray(input_paths));
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
