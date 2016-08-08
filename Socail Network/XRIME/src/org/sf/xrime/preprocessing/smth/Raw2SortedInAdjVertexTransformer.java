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
package org.sf.xrime.preprocessing.smth;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;
import java.util.TreeSet;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.sf.xrime.ProcessorExecutionException;
import org.sf.xrime.Transformer;
import org.sf.xrime.model.edge.Edge;
import org.sf.xrime.model.vertex.AdjVertex;


/**
 * Calculate incoming adjacent vertexes lists for authors from newsmth.net based on article
 * Re: relationships. The incoming data is line-oriented, in text format, the output data
 * serialized according to Writable interface.
 * <p>By "sorted", we mean the adjacent vertexes of a Re:er is sorted alphabetically in the
 * list.</p>
 */
public class Raw2SortedInAdjVertexTransformer extends Transformer{
  /**
   * Default constructor.
   */
  public Raw2SortedInAdjVertexTransformer(){
    super();
  }
  /**
   * Normal constructor.
   * @param src
   * @param dest
   */
  public Raw2SortedInAdjVertexTransformer(Path src, Path dest){
    super(src, dest);
  }
  /**
   * For each input line, emit a friend list, i.e., an incoming adjacent vertex for the
   * author. This list will have redundant entries removed.
   */
  public static class MapClass extends MapReduceBase
    implements Mapper<LongWritable, Text, Text, AdjVertex> {
    
    private Text author = new Text();
    
    public void map(LongWritable key, Text value, 
                    OutputCollector<Text, AdjVertex> output, 
                    Reporter reporter) throws IOException {
      // Get the input line.
      String line = value.toString().trim();
      
      // Get re-ers.
      int reer_startindex = line.lastIndexOf(" ");
      if(reer_startindex < 0 || line.charAt(reer_startindex+1)!='+') return; // Input line is error.
      String reer_line = line.substring(reer_startindex+2);
      
      // Get the original author name.
      String substr_before_reer = line.substring(0, reer_startindex).trim();
      int orig_startindex = substr_before_reer.lastIndexOf(" ");
      if(orig_startindex < 0) return; // Input line is error.
      String author_name = substr_before_reer.substring(orig_startindex+1).trim(); 
      // The original author.
      author.set(author_name);
      
      // Tokenize the re-ers' part.
      StringTokenizer itr = new StringTokenizer(reer_line, "+");
      HashSet<String> friend_set = new HashSet<String>();
      // The Re: authors, i.e., the friends of original author.
      while(itr.hasMoreTokens()){
        String friend_name = itr.nextToken();
        friend_set.add(friend_name);
      }
      
      // Remove the author from the friend list.
      friend_set.remove(author_name);
      
      // Create an instance of AdjVertex.
      AdjVertex adj = new AdjVertex();
      adj.setId(author_name);
      
      // Add those friends as edges to the AdjVertex.
      Iterator<String> iterator;
      for(iterator = friend_set.iterator(); iterator.hasNext();){
        String friend_name = iterator.next();
        Edge edge = new Edge();
        edge.setFrom(friend_name);
        edge.setTo(author_name);
        adj.addEdge(edge);
        // To not omit those who only re others, never originally create articles.
        AdjVertex friend_adj = new AdjVertex();
        friend_adj.setId(friend_name);
        output.collect(new Text(friend_name), friend_adj);
      }
      output.collect(author, adj);
    }
  }
  
  /**
   * Merge the sublists of incoming adjacent vertexes of an author into one.
   */
  public static class ReduceClass extends MapReduceBase
    implements Reducer<Text, AdjVertex, Text, AdjVertex> {
    
    public void reduce(Text key, Iterator<AdjVertex> values,
                       OutputCollector<Text, AdjVertex> output, 
                       Reporter reporter) throws IOException {
      // The set used to accommodate the friends.
      TreeSet<String> friend_set = new TreeSet<String>();
      
      // Each value represents a sublist of friends. Accumulate them into the set.
      while (values.hasNext()) {
        AdjVertex friends = values.next();
        List<Edge> edges = friends.getEdges(); 
        if(edges!=null){
          for(Iterator<Edge> iterator = edges.iterator(); iterator.hasNext();){
            friend_set.add(iterator.next().getFrom());
          }
        }
      }
      
      // Create the AdjVertex instance.
      AdjVertex adj = new AdjVertex();
      String author_name = key.toString();
      // Set the vertex.
      adj.setId(author_name);
      
      // Add friends.
      Iterator<String> iterator;
      for(iterator = friend_set.iterator(); iterator.hasNext();){
        String friend_name = iterator.next();
        Edge edge = new Edge();
        edge.setFrom(friend_name);
        edge.setTo(author_name);
        adj.addEdge(edge);
      }
      output.collect(key, adj);
    }
  }

  @Override
  public void execute() throws ProcessorExecutionException {
    JobConf jobConf =  new JobConf(conf, Raw2SortedInAdjVertexTransformer.class);
    jobConf.setJobName("Smth - SortedInAdjVertexesList");
 
    // the keys are author names (strings)
    jobConf.setOutputKeyClass(Text.class);
    // the values are adjacent vertexes (Writable)
    jobConf.setOutputValueClass(AdjVertex.class);
    
    jobConf.setMapperClass(MapClass.class);        
    jobConf.setCombinerClass(ReduceClass.class);
    jobConf.setReducerClass(ReduceClass.class);
    
    // makes the file format suitable for machine processing.
    jobConf.setOutputFormat(SequenceFileOutputFormat.class);
    
    FileInputFormat.setInputPaths(jobConf, srcPath);
    FileOutputFormat.setOutputPath(jobConf, destPath);
    
    jobConf.setNumMapTasks(mapperNum);
    jobConf.setNumReduceTasks(reducerNum);
    
    try {
      this.runningJob = JobClient.runJob(jobConf);
    } catch (IOException e) {
      throw new ProcessorExecutionException(e);
    }
  }
  
  public static void main(String[] args){
    try {
      int res = ToolRunner.run(new Raw2SortedInAdjVertexTransformer(), args);
      System.exit(res);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
