/*
 * Copyright (C) liuchangyan@BUPT. 2009.
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
package org.sf.xrime.algorithms.layout.radialtree;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;

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
import org.sf.xrime.model.vertex.LabeledAdjVertex;

/**
 * Calculate outgoing adjacent vertexes lists for authors from newsmth.net based on article
 * Re: relationships and add the a label using in  RadialTree Layout Algorithm. The incoming 
 * data is line-oriented, in text format, the output data serialized according to Writable 
 * interface.
 * 
 * We choose to generate these lists from raw data instead of incoming adjacent vertexes
 * list.
 * @author liu chang yan
 */
public class Raw2OutLabeledAdjVertexTransformer extends Transformer{
  /**
   * Default constructor.
   */
  public Raw2OutLabeledAdjVertexTransformer(){
    super();
  }
  /**
   * Normal constructor.
   * @param src
   * @param dest
   */
  public Raw2OutLabeledAdjVertexTransformer(Path src, Path dest){
    super(src, dest);
  }
  /**
   * For each input line, emit a list of AdjVertexes, i.e., a Re:er and its outgoing adjacent
   * vertex (the original author). This list will have redundant entries removed.
   */
  public static class MapClass extends MapReduceBase
    implements Mapper<LongWritable, Text, Text, LabeledAdjVertex> {
    
    private Text friend = new Text();
    
    public void map(LongWritable key, Text value, OutputCollector<Text, LabeledAdjVertex> output, 
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
      
        // Add those friends as a list of AdjVertexes.
        Iterator<String> iterator;
        for(iterator = friend_set.iterator(); iterator.hasNext();){
        String friend_name = iterator.next();
        friend.set(friend_name);
        // Create an instance of AdjVertex.
        LabeledAdjVertex adj = new LabeledAdjVertex();        
        adj.setId(friend_name);
        Edge edge = new Edge();
        edge.setFrom(friend_name);
        edge.setTo(author_name);
        adj.addEdge(edge);
        RadialTreeLabel label=(RadialTreeLabel) adj.getLabel(RadialTreeLabel.RadialTreeLabelPathsKey);
        label=new RadialTreeLabel();
        adj.setLabel(RadialTreeLabel.RadialTreeLabelPathsKey, label);
        output.collect(friend, adj);
        }
      
        // Considering those who only publish articles, not re articles (including those who also
        // never got re'ed).
        LabeledAdjVertex adj = new LabeledAdjVertex();
        adj.setId(author_name);
        output.collect(new Text(author_name), adj);
    }
  }
  
  /**
   * Merge the sublists of outgoing adjacent vertexes of a Re:er into one.
   */
  public static class ReduceClass extends MapReduceBase
    implements Reducer<Text, LabeledAdjVertex, Text, LabeledAdjVertex> {
    
    public void reduce(Text key, Iterator<LabeledAdjVertex> values,
                       OutputCollector<Text, LabeledAdjVertex> output, 
                       Reporter reporter) throws IOException {
      // The set used to accommodate the original authors.
      HashSet<String> orig_author_set = new HashSet<String>();
      
      // Each value represents a list of original authors. Accumulate them into the set.
      while (values.hasNext()) {
      LabeledAdjVertex authors = values.next();
        List<Edge> edges = authors.getEdges(); 
        if(edges!=null){
          for(Iterator<Edge> iterator = edges.iterator(); iterator.hasNext();){
            // Get the original author from the edge and put it into the set.
            orig_author_set.add(iterator.next().getTo());
          }
        }
      }
      
      // Create the AdjVertex instance.
      LabeledAdjVertex adj = new LabeledAdjVertex();
      String friend_name = key.toString();
      // Set the vertex.
      adj.setId(friend_name);
      
      // Add original authors.
      Iterator<String> iterator;
      for(iterator = orig_author_set.iterator(); iterator.hasNext();){
        String author_name = iterator.next();
        Edge edge = new Edge();
        edge.setFrom(friend_name);
        edge.setTo(author_name);
        adj.addEdge(edge);
      }
      // Add RadialTree Label
      RadialTreeLabel label=(RadialTreeLabel) adj.getLabel(RadialTreeLabel.RadialTreeLabelPathsKey);
      label=new RadialTreeLabel();      
      adj.setLabel(RadialTreeLabel.RadialTreeLabelPathsKey, label);
      output.collect(key, adj);
    }
  }
  
  @Override
  public void execute() throws ProcessorExecutionException {
    JobConf jobConf =  new JobConf(conf, Raw2OutLabeledAdjVertexTransformer.class);
    jobConf.setJobName("Smth - OutLabeledAdjVertexesList");
 
    jobConf.setMapperClass(MapClass.class);        
    jobConf.setCombinerClass(ReduceClass.class);
    jobConf.setReducerClass(ReduceClass.class);
    
    // the keys are author names (strings)
    jobConf.setOutputKeyClass(Text.class);
    // the values are adjacent vertexes (Writable)
    jobConf.setOutputValueClass(LabeledAdjVertex.class);
    
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
      int res = ToolRunner.run(new Raw2OutLabeledAdjVertexTransformer(), args);
      System.exit(res);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
