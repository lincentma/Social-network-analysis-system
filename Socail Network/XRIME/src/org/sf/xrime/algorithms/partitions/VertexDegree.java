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
package org.sf.xrime.algorithms.partitions;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.sf.xrime.ProcessorExecutionException;
import org.sf.xrime.algorithms.GraphAlgorithm;
import org.sf.xrime.algorithms.utils.GraphAlgorithmMapReduceBase;
import org.sf.xrime.model.Graph;
import org.sf.xrime.model.vertex.AdjVertex;


/**
 * Calculate degree for each vertex in a graph, which is stored in the form of an adjacent
 * vertexes table. Note: For a directed graph, an adjacent vertexes table may have one of
 * two meanings. A row of the table may represent a vertex and vertexes pointed to by it.
 * Or, it may represent a vertex and vertexes point to it. The former one is what we called
 * outgoing adjacent vertexes list, and corresponding degree is called outgoing degree. 
 * The later one is what we called incoming adjacent vertexes list, and corresponding degree
 * is called incoming degree. The meaning of table is independent from the calculation of
 * the statistics here.
 */
public class VertexDegree extends GraphAlgorithm{
  /**
   * Default constructor.
   */
  public VertexDegree(){
    super();
  }
  /**
   * For each vertex, emit the number of adjacent vertexes.
   */
  public static class MapClass extends GraphAlgorithmMapReduceBase
    implements Mapper<Text, AdjVertex, Text, IntWritable> {
    
    public void map(Text key, AdjVertex value, 
                    OutputCollector<Text, IntWritable> output, 
                    Reporter reporter) throws IOException {
      // Get the degree from the input adjacent vertexes list.
      output.collect(key, new IntWritable(
          (value.getEdges()==null)?0:value.getEdges().size()));
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
    JobConf conf = new JobConf(context, VertexDegree.class);
    conf.setJobName("VertexDegree");
 
    // the keys are authors (strings)
    conf.setOutputKeyClass(Text.class);
    // the values are vertex degree (ints)
    conf.setOutputValueClass(IntWritable.class);
    conf.setMapperClass(MapClass.class);        
    // No combiner is needed.
    // No reducer is needed.
    // The format of input data is generated with WritableSerialization.
    conf.setInputFormat(SequenceFileInputFormat.class);
    try {
      FileInputFormat.setInputPaths(conf, getSource().getPath());
      FileOutputFormat.setOutputPath(conf, getDestination().getPath());
    } catch (IllegalAccessException e1) {
      throw new ProcessorExecutionException(e1);
    }
    conf.setNumMapTasks(getMapperNum());
    conf.setNumReduceTasks(0);
    
    try {
      this.runningJob = JobClient.runJob(conf);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
  
  public static void main(String[] args){
    try {
      int res = ToolRunner.run(new VertexDegree(), args);
      System.exit(res);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
