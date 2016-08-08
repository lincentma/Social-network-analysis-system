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
import org.sf.xrime.model.edge.AdjVertexEdge;
import org.sf.xrime.model.path.PathAsVertexesList;
import org.sf.xrime.model.vertex.LabeledAdjSetVertex;
import org.sf.xrime.model.vertex.Vertex;


/**
 * Generate a spanning tree by bread first searching (BFS). Currently do not use the BFS
 * algorithm, and will change to using it in the future.
 * @author xue
 */
public class SpanningTreeGenerate extends GraphAlgorithm {
  /**
   * Default constructor.
   */
  public SpanningTreeGenerate(){
    super();
  }
  /**
   * Mapper is used to propagate the id of the root of the spanning tree to all other
   * nodes in the tree. Each vertex in the graph could be in one of three states during
   * this propagation (when at the end of each round of propagation): hasn't been included
   * in the spanning tree; in the spanning tree but not at the frontier; in the spanning
   * tree and at the frontier. The indications of these states are:
   * 
   * SPANNING_DONE==null && PATH_TO_ROOT==null
   * SPANNING_DONE!=null (implies that PATH_TO_ROOT!=null)
   * SPANNING_DONE==null && PATH_TO_ROOT!=null
   * 
   * @author xue
   */
  public static class MapClass extends GraphAlgorithmMapReduceBase implements
    Mapper<Text, LabeledAdjSetVertex, Text, LabeledAdjSetVertex>{

    @Override
    public void map(Text key, LabeledAdjSetVertex value,
        OutputCollector<Text, LabeledAdjSetVertex> output, Reporter reporter)
        throws IOException {
      // Get the id of the root of the targeting spanning tree.
      String root_id = this.context.getParameter(ConstantLabels.ROOT_ID);
      if(value.getLabel(ConstantLabels.SPANNING_DONE)==null){
        // The vertex hasn't propagate the path to root to its neightbors.
        if(key.toString().compareTo(root_id)==0){
          // This vertex is the root of the spanning tree. This must be the starting point
          // of spanning tree generation.
          
          // Generate the path to root for this vertex. The only vertex in it is the
          // root vertex itself.
          PathAsVertexesList path_to_root = new PathAsVertexesList();
          // We choose to use the simplest form of vertex here.
          path_to_root.addVertex(new Vertex(value));
          value.setLabel(ConstantLabels.PATH_TO_ROOT, path_to_root);
          // Generate pathes to root for all neightbours of this vertex and notify
          // them about this.
          for(AdjVertexEdge oppo : value.getOpposites()){
            PathAsVertexesList oppo_path_to_root = new PathAsVertexesList();
            // Add the subpath from root to this vertex first. No need to do deep clone.
            oppo_path_to_root.addVertexes(path_to_root.getVertexes());
            // Add the last step. Use the simplest form here too.
            oppo_path_to_root.addVertex(new Vertex(oppo.getOpposite()));
            // Generate notifiers.
            LabeledAdjSetVertex notifier = new LabeledAdjSetVertex();
            notifier.setId(oppo.getOpposite());
            // Specify the path_to_root recommendations. It's necessary to distinguish this
            // from the final path_to_root.
            notifier.setLabel(ConstantLabels.POTENTIAL_PATH_TO_ROOT, oppo_path_to_root);
            // Notify the opposite vertex.
            output.collect(new Text(oppo.getOpposite()), notifier);
          }
          // Mark this vertex as spanning done.
          value.setStringLabel(ConstantLabels.SPANNING_DONE, "done");
        }else{
          // This vertex is a non-root vertex of the spanning tree and currently a part of
          // the frontier of the tree.
          if(value.getLabel(ConstantLabels.PATH_TO_ROOT)!=null){
            // Get the path to root for this vertex.
            PathAsVertexesList path_to_root = 
              (PathAsVertexesList) value.getLabel(ConstantLabels.PATH_TO_ROOT);
            // Generate pathes to root for all neighbours of this vertex and notify them
            // about this.
            for(AdjVertexEdge oppo : value.getOpposites()){
              // Skip the neighbour which is also the last node within the path to root.
              if(oppo.getOpposite().compareTo(
                  path_to_root.getVertexes().get(path_to_root.getVertexes().size()-1).getId())==0) continue;
              // Similar with above.
              PathAsVertexesList oppo_path_to_root = new PathAsVertexesList();
              oppo_path_to_root.addVertexes(path_to_root.getVertexes());
              oppo_path_to_root.addVertex(new Vertex(oppo.getOpposite()));
              LabeledAdjSetVertex notifier = new LabeledAdjSetVertex();
              notifier.setId(oppo.getOpposite());
              notifier.setLabel(ConstantLabels.POTENTIAL_PATH_TO_ROOT, oppo_path_to_root);
              output.collect(new Text(oppo.getOpposite()), notifier);
            }
            // Mark this vertex as spanning done.
            value.setStringLabel(ConstantLabels.SPANNING_DONE, "done");
          }else{
            // This vertex hasn't got into the spanning tree. So, it has no recommendations
            // to its neighbours. Do nothing.
          }
        }
      }else{
        // The vertex has already propagate the path to root to its neightbors. In another word,
        // This vertex is no longer the frontier of the spanning tree. Do nothing.
      }
      // Emit the vertex itself, whose status might be updated.
      output.collect(key, value);
    }
  }
  
  /**
   * Reducer is used to finish the propagation of this step.
   * @author xue
   */
  public static class ReduceClass extends GraphAlgorithmMapReduceBase implements
    Reducer<Text, LabeledAdjSetVertex, Text, LabeledAdjSetVertex>{

    @Override
    public void reduce(Text key, Iterator<LabeledAdjSetVertex> values,
        OutputCollector<Text, LabeledAdjSetVertex> output, Reporter reporter)
        throws IOException {
      LabeledAdjSetVertex result = new LabeledAdjSetVertex();
      result.setId(key.toString());
      PathAsVertexesList path_to_root = null;
      while(values.hasNext()){
        LabeledAdjSetVertex curr_vertex = values.next();
        if(curr_vertex.getLabel(ConstantLabels.PATH_TO_ROOT)!=null){
          // This vertex has already been met during the spanning of the tree.
          // So, just emit its current form, which may or may not already propagate
          // the root id to the neighbours.
          output.collect(key, curr_vertex);
          // Just stop here.
          return;
        }else{
          // We need to accumulate stuffs in case that this is the first time
          // we meet this vertex during the spanning of the tree.
          
          // Summarize all incidental edges.
          for(AdjVertexEdge oppo : curr_vertex.getOpposites()){
            result.addOpposite((AdjVertexEdge) oppo.clone());
          }
          if(path_to_root == null &&
             curr_vertex.getLabel(ConstantLabels.POTENTIAL_PATH_TO_ROOT)!=null){
            // Only collect one path to root, since what we want to build is a spanning tree, not
            // spanning graph with circles.
            path_to_root = (PathAsVertexesList) 
                           ((PathAsVertexesList) 
                            curr_vertex.getLabel(ConstantLabels.POTENTIAL_PATH_TO_ROOT)).clone();
          }
        }
      }
      // If comes here, this vertex hasn't been met before. So, set the path to root for this
      // vertex. NOTE: path_to_root here may be null if none recommendation is proposed.
      if(path_to_root!=null)
        result.setLabel(ConstantLabels.PATH_TO_ROOT, path_to_root);
      output.collect(key, result);
    }
  }
  
  @Override
  public void execute() throws ProcessorExecutionException {
    JobConf conf = new JobConf(context, SpanningTreeGenerate.class);
    conf.setJobName("SpanningTreeGenerate");
    
    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(LabeledAdjSetVertex.class);
    conf.setMapperClass(MapClass.class);        
    // No combiner is permitted, since the logic of reducer depends on the completeness
    // of information.
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
    conf.setMapOutputCompressorClass(GzipCodec.class);
    conf.setCompressMapOutput(true);
        
    try {
      this.runningJob = JobClient.runJob(conf);
    } catch (IOException e) {
      throw new ProcessorExecutionException(e);
    }
  }
}
