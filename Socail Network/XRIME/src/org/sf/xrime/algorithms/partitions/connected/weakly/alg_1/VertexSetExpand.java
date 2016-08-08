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
package org.sf.xrime.algorithms.partitions.connected.weakly.alg_1;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
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
import org.apache.hadoop.util.ToolRunner;
import org.sf.xrime.ProcessorExecutionException;
import org.sf.xrime.algorithms.GraphAlgorithm;
import org.sf.xrime.algorithms.utils.GraphAlgorithmMapReduceBase;
import org.sf.xrime.model.Graph;
import org.sf.xrime.model.vertex.Vertex;
import org.sf.xrime.model.vertex.VertexSet;


/**
 * Expand vertex sets of lower layers. Or, in another word, propagate the label
 * of a weakly connected component to more vertexes in it.
 * @author xue
 */
public class VertexSetExpand extends GraphAlgorithm {
  /**
   * Default constructor.
   */
  public VertexSetExpand(){
    super();
  }
  /**
   * Emit input key/value.
   * @author xue
   */
  public static class MapClass extends GraphAlgorithmMapReduceBase implements
    Mapper<Text, VertexSet, Text, VertexSet>{
    @Override
    public void map(Text key, VertexSet value,
        OutputCollector<Text, VertexSet> output, Reporter reporter)
        throws IOException {
      // It should be safe to not clone the vertex set here, since we
      // collect it immediately.
      output.collect(key, value);
    }
    
  }
  
  /**
   * Do the propagation.
   * @author xue
   */
  public static class ReduceClass extends GraphAlgorithmMapReduceBase implements
    Reducer<Text, VertexSet, Text, VertexSet>{

    @Override
    public void reduce(Text key, Iterator<VertexSet> values,
        OutputCollector<Text, VertexSet> output, Reporter reporter)
        throws IOException {
      Vertex label = null;
      HashSet<String> id_set = new HashSet<String>();
      while (values.hasNext()) {
        VertexSet curr_set = values.next();
        if (curr_set.getVertexes().size() == 1) {
          // Get the vertex.
          Vertex temp_vertex = (Vertex) curr_set.getVertexes().toArray()[0];
          if (label == null) {
            // Update the label and find the right one finally.
            label = temp_vertex;
          }else if(temp_vertex.getId().compareTo(label.getId())<0){
            // Update the label and find the right one finally.
            label = temp_vertex;
          }else{
            // Do nothing.
          }
        }
        // Collect all vertex ids.
        for (Vertex curr_vertex : curr_set.getVertexes()) {
          id_set.add(curr_vertex.getId());
        }
      }
      // label should not be null here.
      VertexSet label_set = new VertexSet();
      label_set.addVertex(label);
      for(String id: id_set){
        output.collect(new Text(id), label_set);
      }
    }
  }
  
  @Override
  public void setArguments(String[] params) throws ProcessorExecutionException {
    // Make sure there are exactly 3 parameters left.
    if (params.length != 3) {
      throw new ProcessorExecutionException("Wrong number of parameters: " +
                         params.length + " instead of 3.");
    }
    
    // Configure the algorithm instance.
    Graph src = new Graph(Graph.defaultGraph());
    src.addPath(new Path(params[0]));
    src.addPath(new Path(params[1]));
    Graph dest = new Graph(Graph.defaultGraph());
    dest.setPath(new Path(params[2]));
    
    setSource(src);
    setDestination(dest);
  }

  @Override
  public void execute() throws ProcessorExecutionException {
    JobConf conf = new JobConf(context, VertexSetExpand.class);
    conf.setJobName("VertexSetExpand");
    
    // the keys are vertex identifiers (strings)
    conf.setOutputKeyClass(Text.class);
    // the values are ultimate labels of vertexes (Writable)
    conf.setOutputValueClass(VertexSet.class);
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
        
    try {
      this.runningJob = JobClient.runJob(conf);
    } catch (IOException e) {
      throw new ProcessorExecutionException(e);
    }
  }
  
  /**
   * @param args
   */
  public static void main(String[] args) {
    try {
      int res = ToolRunner.run(new VertexSetExpand(), args);
      System.exit(res);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
