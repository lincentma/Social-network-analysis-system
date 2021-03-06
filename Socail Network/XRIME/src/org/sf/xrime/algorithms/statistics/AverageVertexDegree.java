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
package org.sf.xrime.algorithms.statistics;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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
import org.apache.hadoop.util.ToolRunner;
import org.sf.xrime.ProcessorExecutionException;
import org.sf.xrime.algorithms.GraphAlgorithm;
import org.sf.xrime.algorithms.utils.GraphAlgorithmMapReduceBase;
import org.sf.xrime.model.Graph;
import org.sf.xrime.model.vertex.AdjVertex;


/**
 * Calculate average degree of vertexes in the graph.
 */
public class AverageVertexDegree extends GraphAlgorithm{
  /**
   * Default constructor.
   */
  public AverageVertexDegree(){
    super();
  }
  /**
   * Emit the degree of each vertex.
   */
  public static class MapClass extends GraphAlgorithmMapReduceBase
    implements Mapper<Text, AdjVertex, Text, IntWritable> {
    
    public void map(Text key, AdjVertex value, 
                    OutputCollector<Text, IntWritable> output, 
                    Reporter reporter) throws IOException {
      // Get the degree from the input adjacent vertexes list.
      output.collect(new Text("Average_Degree"), new IntWritable(
          (value.getEdges()==null)?0:value.getEdges().size()));
      }
  }
  
  /**
   * Calculate the average degree.
   */
  public static class ReduceClass extends GraphAlgorithmMapReduceBase
    implements Reducer<Text, IntWritable, Text, IntWritable> {
    
    public void reduce(Text key, Iterator<IntWritable> values,
                       OutputCollector<Text, IntWritable> output, 
                       Reporter reporter) throws IOException {
      int count = 0;
      int sum = 0;
      while (values.hasNext()) {
        count++;
        sum += values.next().get();
      }
      if (count==0) count=1;
      output.collect(key, new IntWritable(sum/count));
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
    JobConf conf = new JobConf(context, AverageVertexDegree.class);
    conf.setJobName("AverageDegree");
    
    // the keys are a pseudo one ("Average_Degree")
    conf.setOutputKeyClass(Text.class);
    // the values are degrees (ints)
    conf.setOutputValueClass(IntWritable.class);
    conf.setMapperClass(MapClass.class);        
    // No combiner is permitted.
    conf.setReducerClass(ReduceClass.class);
    // The format of input data is generated with WritableSerialization.
    conf.setInputFormat(SequenceFileInputFormat.class);
    try {
      FileInputFormat.setInputPaths(conf, getSource().getPath());
      FileOutputFormat.setOutputPath(conf, getDestination().getPath());
    } catch (IllegalAccessException e1) {
      throw new ProcessorExecutionException(e1);
    }
    conf.setNumMapTasks(getMapperNum());
    // Only one reducer is permitted, or the average value will be wrong.
    conf.setNumReduceTasks(1);
    conf.setCompressMapOutput(true);
    conf.setMapOutputCompressorClass(GzipCodec.class);
    
    try {
      this.runningJob = JobClient.runJob(conf);
    } catch (IOException e) {
      throw new ProcessorExecutionException(e);
    }
  }
  
  public static void main(String[] args){
    try {
      int res = ToolRunner.run(new AverageVertexDegree(), args);
      System.exit(res);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
