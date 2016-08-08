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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

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
import org.sf.xrime.model.vertex.AdjVertex;
import org.sf.xrime.model.vertex.Vertex;
import org.sf.xrime.model.vertex.VertexSet;

/**
 * Get the adjacent vertexes lists for a specified component (a subgraph). We have three 
 * inputs: the first one is the original adjacent vertexes lists, the second one is the
 * partition of the graph, the third one is the component to be extracted. The third one
 * is specified as a command line parameter.
 * @author xue
 */
public class AdjVertex4Component extends GraphAlgorithm {
  /**
   * A label name used by this algorithm.
   */
  public static final String TARGET_COMPONENT_LABEL = "org.sf.xrime.target_component_label";
  /**
   * Default constructor.
   */
  public AdjVertex4Component(){
    super();
  }
  /**
   * Emit the adjacent vertexes lists for those vertexes within the component with
   * specified label (which is in turn specified with a configuration parameter).
   * @author xue
   */
  public static class MapClass extends GraphAlgorithmMapReduceBase implements
    Mapper<Text, Object, Text, AdjVertex>{
    @Override
    public void map(Text key, Object value,
        OutputCollector<Text, AdjVertex> output, Reporter reporter)
        throws IOException {
      if(value instanceof AdjVertex){
        // The adjacent vertexes list should be emitted.
        output.collect(key, (AdjVertex)value);
      }else if(value instanceof VertexSet){
        String component_label = ((Vertex)((VertexSet)value).getVertexes().toArray()[0]).getId();
        String the_label = context.getParameter(TARGET_COMPONENT_LABEL);
        if(component_label.compareTo(the_label)==0){
          // If the vertex is in the component, emit a pseudo adjacent vertexes list.
          // As a result, we can use this as the criteria in reducer.
          AdjVertex one_more_vertex = new AdjVertex();
          one_more_vertex.setId(key.toString());
          output.collect(key, one_more_vertex);
        }
      }
    }
  }
  /**
   * Filter and retain the adjacent vertexes lists for vertexes within specified component.
   * @author xue
   */
  public static class ReduceClass extends GraphAlgorithmMapReduceBase implements
    Reducer<Text, AdjVertex, Text, AdjVertex>{

    @Override
    public void reduce(Text key, Iterator<AdjVertex> values,
        OutputCollector<Text, AdjVertex> output, Reporter reporter)
        throws IOException {
      AdjVertex[] vertexes = new AdjVertex[2];
      int count = 0;
      // There are no more than 2 of them.
      while(values.hasNext()&&count<2){
        // Clone the input AdjVertex.
        vertexes[count] = new AdjVertex(values.next());
        count++;
      }
      if(count==2){
        // The vertex belongs to the specified component.
        if(vertexes[0].getEdges().size()!=0){
          output.collect(key, vertexes[0]);
        }else if(vertexes[1].getEdges().size()!=0){
          output.collect(key, vertexes[1]);
        }else{
          // A special case, the vertex only has inbound or outbound
          // edges, which depends the type of adjacent vertexes lists
          // (inbound or outboud).
          output.collect(key, vertexes[0]);
        }
      }
    }
  }
  
  @Override
  public void setArguments(String[] params) throws ProcessorExecutionException {
    List<String> other_args = new ArrayList<String>();
    String target_component_label = null;
    for(int i=0; i < params.length; ++i) {
      try {
        if ("-c".equals(params[i])) {
          // Specify the component label.
          target_component_label = params[++i];
        } else {
          other_args.add(params[i]);
        }
      } catch (NumberFormatException except) {
        throw new ProcessorExecutionException(except);
      } catch (ArrayIndexOutOfBoundsException except) {
        throw new ProcessorExecutionException(except);
      }
    }
    if(target_component_label == null){
      throw new ProcessorExecutionException("You should specify the target component to be extracted.");
    }
    // Pass the target component label as a configuration parameter.
    setParameter(TARGET_COMPONENT_LABEL, target_component_label);
    // Make sure there are exactly 3 parameters left.
    if (other_args.size() != 3) {
      throw new ProcessorExecutionException("Wrong number of parameters: " +
                         other_args.size() + " instead of 3.");
    }
    
    // Configure the algorithm instance.
    Graph src = new Graph(Graph.defaultGraph());
    src.addPath(new Path(other_args.get(0)));
    src.addPath(new Path(other_args.get(1)));
    Graph dest = new Graph(Graph.defaultGraph());
    dest.setPath(new Path(other_args.get(2)));
    setSource(src);
    setDestination(dest);
  }

  @Override
  public void execute() throws ProcessorExecutionException {
    JobConf conf = new JobConf(context, AdjVertex4Component.class);
    conf.setJobName("AdjVertexesList4Component");
    
    // the keys are vertex identifiers (strings)
    conf.setOutputKeyClass(Text.class);
    // the values are vertex sets (Writable)
    conf.setOutputValueClass(AdjVertex.class);
    conf.setMapperClass(MapClass.class);        
    // No combiner is permitted.
    conf.setReducerClass(ReduceClass.class);
    // makes the file format suitable for machine processing.
    conf.setInputFormat(SequenceFileInputFormat.class);
    conf.setOutputFormat(SequenceFileOutputFormat.class);
    try {
      // The first two directories are original adjacent vertexes lists and partitions of the
      // graph (output of VertexSetExpand and VertexSetMinorExpand).
      Path[] input_paths = new Path[2];
      FileInputFormat.setInputPaths(conf, getSource().getPaths().toArray(input_paths));
      FileOutputFormat.setOutputPath(conf, getDestination().getPath());
    } catch (IllegalAccessException e1) {
      throw new ProcessorExecutionException(e1);
    }
    // Task numbers.
    conf.setNumMapTasks(getMapperNum());
    conf.setNumReduceTasks(getReducerNum());
    
    try {
      this.runningJob = JobClient.runJob(conf);
    } catch (IOException e) {
      throw new ProcessorExecutionException(e);
    }
  }
  
  public static void main(String[] args){
    try {
      int res = ToolRunner.run(new AdjVertex4Component(), args);
      System.exit(res);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
