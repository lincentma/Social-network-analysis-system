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
package org.sf.xrime.algorithms.transform.vertex;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.sf.xrime.ProcessorExecutionException;
import org.sf.xrime.Transformer;
import org.sf.xrime.model.edge.Edge;
import org.sf.xrime.model.vertex.AdjVertex;
import org.sf.xrime.model.vertex.Vertex;
import org.sf.xrime.model.vertex.VertexSet;


/**
 * <p>Transform the adjacent vertexes list of each vertex in the graph into a vertex set, which
 * cover the vertex and all its neighbours. We don't care the input adjacent vertexes list is
 * "incoming" or "outgoing". The resulting set will cover both of them. </p>
 * 
 * <p>In this way, we get a coverage of the graph, which consists of those vertex sets.</p>
 * @author xue
 */
public class AdjVertex2VertexSetTransformer extends Transformer {
  /**
   * Default constructor.
   */
  public AdjVertex2VertexSetTransformer(){
    super();
  }
  /**
   * Another constructor.
   * @param src
   * @param dest
   */
  public AdjVertex2VertexSetTransformer(Path src, Path dest){
    super(src, dest);
  }
  /**
   * Emit a vertex (key) and its surrounding neighbours (value).
   * @author xue
   */
  public static class MapClass extends MapReduceBase implements 
    Mapper<Text, AdjVertex, Text, VertexSet>{

    @Override
    public void map(Text key, AdjVertex value,
        OutputCollector<Text, VertexSet> output, Reporter reporter)
        throws IOException {
      // The "forward" adjacent vertex set.
      VertexSet forward_set = new VertexSet();
      // Add the key vertex itself.
      forward_set.addVertex(new Vertex(key.toString()));
      // The "backward" vertex set.
      VertexSet backward_set = new VertexSet();
      // Add the key vertex itself.
      backward_set.addVertex(new Vertex(key.toString()));
      // Get all edges.
      List<Edge> edges = value.getEdges();
      // Process each edge in the adjacent vertexes list.
      for(Edge edge : edges){
        forward_set.addVertex(new Vertex(edge.getFrom()));
        output.collect(new Text(edge.getFrom()), backward_set);
        forward_set.addVertex(new Vertex(edge.getTo()));
        output.collect(new Text(edge.getTo()), backward_set);
      }
      output.collect(key, forward_set);
    }
  }
  
  /**
   * Create the vertex set cooresponding to each vertex and its neighbours. In another
   * word, merge neighbours (value) of a vertex (key) into a vertex set.
   * @author xue
   */
  public static class ReduceClass extends MapReduceBase implements
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
  public void execute() throws ProcessorExecutionException {
    JobConf jobConf =  new JobConf(conf, AdjVertex2VertexSetTransformer.class);
    jobConf.setJobName("AdjVertex2VertexSetTransformer");
 
    // the keys are vertex identifiers (strings)
    jobConf.setOutputKeyClass(Text.class);
    // the values are vertex sets (Writable)
    jobConf.setOutputValueClass(VertexSet.class);
    jobConf.setMapperClass(MapClass.class);        
    jobConf.setCombinerClass(ReduceClass.class);
    jobConf.setReducerClass(ReduceClass.class);
    // makes the file format suitable for machine processing.
    jobConf.setInputFormat(SequenceFileInputFormat.class);
    jobConf.setOutputFormat(SequenceFileOutputFormat.class);
    FileInputFormat.setInputPaths(jobConf, srcPath);
    FileOutputFormat.setOutputPath(jobConf, destPath);
    jobConf.setNumMapTasks(mapperNum);
    jobConf.setNumReduceTasks(reducerNum);
    jobConf.setCompressMapOutput(true);
    jobConf.setMapOutputCompressorClass(GzipCodec.class);
    
    try {
      this.runningJob = JobClient.runJob(jobConf);
    } catch (IOException e) {
      throw new ProcessorExecutionException(e);
    }
  }
  
  /**
   * @param args
   */
  public static void main(String[] args) {
    try {
      int res = ToolRunner.run(new AdjVertex2VertexSetTransformer(), args);
      System.exit(res);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
