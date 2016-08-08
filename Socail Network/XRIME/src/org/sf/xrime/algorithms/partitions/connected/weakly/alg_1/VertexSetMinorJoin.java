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
import java.util.Iterator;
import java.util.TreeSet;

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
 * Re-organize the covering vertex sets of a graph. Use the lowest/smallest vertex id
 * to identify and merge those vertex sets. In this way, we get a new coverage of the
 * graph.
 * @author xue
 */
public class VertexSetMinorJoin extends GraphAlgorithm {
  /**
   * Default constructor.
   */
  public VertexSetMinorJoin(){
    super();
  }
  /**
   * Sort vertexes in each vertex set according to their id. Then use the lowest/smallest
   * id in the set as the key, the whole set as the value and emit the pair.
   * @author xue
   */
  public static class MapClass extends GraphAlgorithmMapReduceBase implements
    Mapper<Text, VertexSet, Text, VertexSet>{

    @Override
    public void map(Text key, VertexSet value,
        OutputCollector<Text, VertexSet> output, Reporter reporter)
        throws IOException {
      TreeSet<String> id_set = new TreeSet<String>();
      for(Vertex vertex: value.getVertexes()){
        id_set.add(vertex.getId());
      }
      // It should be ok not to clone the VertexSet, since we collect
      // it immediately, which means the content of the object "value"
      // is serialized out immediately.
      output.collect(new Text(id_set.first()), value);
    }
  }
  
  /**
   * Merge vertex sets accroding to the "new" key, i.e., the lowest/smallest
   * vertex set in each vertex set. 
   * @author xue
   */
  public static class ReduceClass extends GraphAlgorithmMapReduceBase implements
    Reducer<Text, VertexSet, Text, VertexSet>{
    @Override
    public void reduce(Text key, Iterator<VertexSet> values,
        OutputCollector<Text, VertexSet> output, Reporter reporter)
        throws IOException {
      // The set used to accommodate all sub sets.
      VertexSet result_set = new VertexSet();
      // Merge all the sub sets.
      while (values.hasNext()) {
        VertexSet curr_set = values.next();
        for (Vertex curr_vert : curr_set.getVertexes()) {
          // It should be ok to not clone the vertex.
          result_set.addVertex(curr_vert);
        }
      }
      // Output the result.
      output.collect(key, result_set);
    }
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

  @Override
  public void execute() throws ProcessorExecutionException {
    JobConf conf = new JobConf(context, VertexSetMinorJoin.class);
    conf.setJobName("VertexSetMinorJoin");
    
    // the keys are vertex identifiers (strings)
    conf.setOutputKeyClass(Text.class);
    // the values are vertex sets (Writable)
    conf.setOutputValueClass(VertexSet.class);
    conf.setMapperClass(MapClass.class);        
    // Combiner is permitted here.
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
      int res = ToolRunner.run(new VertexSetMinorJoin(), args);
      System.exit(res);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
