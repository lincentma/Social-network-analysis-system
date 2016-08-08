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
package org.sf.xrime.algorithms.clique.maximal;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
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
import org.apache.hadoop.util.ToolRunner;
import org.sf.xrime.ProcessorExecutionException;
import org.sf.xrime.algorithms.GraphAlgorithm;
import org.sf.xrime.algorithms.utils.GraphAlgorithmMapReduceBase;
import org.sf.xrime.model.Graph;
import org.sf.xrime.model.edge.AdjVertexEdge;
import org.sf.xrime.model.edge.Edge;
import org.sf.xrime.model.edge.EdgeSet;
import org.sf.xrime.model.vertex.LabeledAdjSetVertex;
import org.sf.xrime.model.vertex.SetOfVertexSets;
import org.sf.xrime.model.vertex.SortedVertexSet;
import org.sf.xrime.model.vertex.Vertex;
import org.sf.xrime.model.vertex.VertexSet;


/**
 * This algorithm is used as the final step to generate all maximal cliques in a graph.
 * @author xue
 */
public class AllMaximalCliquesGenerate extends GraphAlgorithm {
  /**
   * Default constructor.
   */
  public AllMaximalCliquesGenerate(){
    super();
  }

  /**
   * Mapper. Generate maximal cliques containing each vertex.
   * @author xue
   *
   */
  public static class MapClass extends GraphAlgorithmMapReduceBase implements
    Mapper<Text, LabeledAdjSetVertex, Text, SetOfVertexSets>{

    @Override
    public void map(Text key, LabeledAdjSetVertex value,
        OutputCollector<Text, SetOfVertexSets> output, Reporter reporter)
        throws IOException {
      // Get the neighbors of this vertex as a list of nodes sorted in lexical order.
      TreeSet<String> set_of_neighbors = new TreeSet<String>();
      for(AdjVertexEdge oppo : value.getOpposites()){
        set_of_neighbors.add(oppo.getOpposite());
      }
      
      // Make it a list.
      ArrayList<String> list_of_neighbors = new ArrayList<String>();
      list_of_neighbors.addAll(set_of_neighbors);
      
      // Get the induced neighborhood of this vertex as a set of Edges.
      Set<Edge> induced_neighborhood = ((EdgeSet) value.getLabel(ConstantLabels.INDUCED_NEIGHBORHOOD)).getEdges();
      
      // Reconstruct the neighborhood for each neighbor of this vertex.
      HashMap<String, HashSet<String>> NoN = ReconstructNoN(list_of_neighbors, induced_neighborhood);
      // Make some noise.
      reporter.progress();
      
      // Generate all maximal cliques within the induced neighborhood of this vertex.
      HashSet<HashSet<String>> cliques = Generate_Maximal_Cliques(list_of_neighbors, NoN, reporter);
      
      // Stupid type wrapping work.
      SetOfVertexSets result = new SetOfVertexSets();
      for(HashSet<String> clique : cliques){
        SortedVertexSet temp_vertex_set = new SortedVertexSet();
        temp_vertex_set.addVertex(new Vertex(key.toString()));
        for(String id : clique){
          temp_vertex_set.addVertex(new Vertex(id));
        }
        result.addVertexSet(temp_vertex_set);
      }
      
      // Collect it.
      output.collect(new Text(ConstantLabels.ALL_MAXIMAL_CLIQUES), result);
    }
    
    /**
     * Reconstruct the neighborhood for each neighbor of this vertex. Actually, the neighborhood
     * for each neighbor is not the original full one, it is the intersection of the original
     * full one with the neighborhood of this vertex.
     * @param list_of_neighbors list of neighbors of this vertex.
     * @param induced_neighborhood the induced neighborhood of this vertex.
     * @return
     */
    public HashMap<String, HashSet<String>> ReconstructNoN(ArrayList<String> list_of_neighbors,
        Set<Edge> induced_neighborhood){
      HashMap<String, HashSet<String>> result = new HashMap<String, HashSet<String>>();
      
      // Create an empth neighbor set for each neighbor of this vertex.
      for(String n_id : list_of_neighbors){
        HashSet<String> temp_set = new HashSet<String>();
        result.put(n_id, temp_set);
      }
      
      // Construct NoN.
      for(Edge edge : induced_neighborhood){
        String from = edge.getFrom();
        String to = edge.getTo();
        result.get(from).add(to);
        result.get(to).add(from);
      }
      
      return result;
    }
    
    /**
     * Generate cliques of size j+1 from cliques of size j.
     * @param node_list lexically sorted list of neighbors of this vertex.
     * @param NoN neighborhood of neighbors.
     * @param reporter reporter.
     * @param j j+1 = the size of input cliques.
     * @param c_j input cliques.
     * @return
     */
    @SuppressWarnings("unchecked")
    public HashSet<HashSet<String>> Generate(List<String> node_list, HashMap<String, HashSet<String>> NoN,
        Reporter reporter, int j, HashSet<HashSet<String>> c_j){
      HashSet<HashSet<String>> result = new HashSet<HashSet<String>>();
      String next_node = node_list.get(j+1);
      
      // Deal with each input clique.
      for(HashSet<String> clique : c_j){
        boolean found_bigger_clique = true;
        // Check every vertex in this input clique, in order to determine whether adding the next
        // node could help to construct a bigger clique.
        if(!(NoN.get(next_node).containsAll(clique))){
          found_bigger_clique = false;
        }
        /**
         * Old way to check.
         * 
        for(String id : clique){
          // According to our arrangement, id will be lexically less than next_node. But this 
          // doesn't matter.
          if(!(NoN.get(id).contains(next_node))){
            found_bigger_clique = false;
            break;
          }
        }
        */
        
        if(found_bigger_clique){
          // Adding the next node just creates a new bigger clique.
          HashSet<String> new_clique = (HashSet<String>) clique.clone();
          // Add the next node too.
          new_clique.add(next_node);
          result.add(new_clique);
        }else{
          // Adding the next node does not create a new bigger clique.
          // Add the input clique first.
          HashSet<String> new_clique_1 = (HashSet<String>) clique.clone();
          result.add(new_clique_1);
          
          // Generate another clique.
          HashSet<String> new_clique_2 = (HashSet<String>) clique.clone();
          // Get the intersection of input clique and N(j+1).
          new_clique_2.retainAll(NoN.get(next_node));
          // Add the next node (i.e., j+1).
          new_clique_2.add(next_node);
          
          // Check for lemma 2.
          boolean is_maximal_clique = true;
          // Check each vertex indexed from 0 to j.
          for(int k=0; k<=j; k++){
            // Pay attention to those which may not be included in new_clique_2.
            if(!clique.contains(node_list.get(k))){
              HashSet<String> temp_set = (HashSet<String>) NoN.get(node_list.get(k)).clone();
              temp_set.retainAll(new_clique_2);
              if(temp_set.size()==new_clique_2.size()&&
                 temp_set.containsAll(new_clique_2)){
                // N(k) intersect C' == C', which makes C' (new_clique_2) not
                // maximal clique belongs to Cj+1.
                is_maximal_clique = false;
                break;
              }
            }
          }
          
          if(is_maximal_clique){
            result.add(new_clique_2);
          }
        }
      }
      return result;
    }
    
    /**
     * Generate all maximal cliques from the induced neighborhood of this vertex.
     * @param neighbors list of neighbors.
     * @param NoN neighborhood for each neighbor of this vertex. Each neighborhood is within the
     * induced neighborhood of this vertex.
     * @param reporter      
     * @return
     */
    public HashSet<HashSet<String>> Generate_Maximal_Cliques(List<String> neighbors, 
        HashMap<String, HashSet<String>> NoN, Reporter reporter){
      HashSet<HashSet<String>> result = new HashSet<HashSet<String>>();
      
      // Prepare the starting point, aka., the clique with size 1.
      HashSet<String> starting_point = new HashSet<String>();
      starting_point.add(neighbors.get(0));
      result.add(starting_point);
      
      for(int j=0; j<neighbors.size()-1; j++){
        result = Generate(neighbors, NoN, reporter, j, result);
        reporter.progress();
      }
      
      return result;
    }
  }
  
  /**
   * Reducer. Accumulate maximal cliques come from each vertex and merge them into
   * the final maximal cliques set.
   * @author xue
   */
  public static class ReduceClass extends GraphAlgorithmMapReduceBase implements
    Reducer<Text, SetOfVertexSets, Text, Text>{
    
    /**
     * A comparator used to compare two sorted string set according to their sizes firstly,
     * and lexical order secondly. Use descending size order, ascending lexical order.
     * @author xue
     */
    public class IdSetComparator implements Comparator<TreeSet<String>> {
      @Override
      public int compare(TreeSet<String> o1, TreeSet<String> o2) {
        if(o1.size()>o2.size()){
          return -1;
        }else if (o1.size()==o2.size()){
          String[] array1 = new String[o1.size()];
          array1 = o1.toArray(array1);
          String[] array2 = new String[o2.size()];
          array2 = o2.toArray(array2);
          
          for(int i=0; i<o1.size(); i++){
            if(array1[i].compareTo(array2[i])==0){
              continue;
            }else{
              return array1[i].compareTo(array2[i]);
            }
          }
          return 0;
        }else{
          return 1;
        }
      }
    }

    @Override
    public void reduce(Text key, Iterator<SetOfVertexSets> values,
        OutputCollector<Text, Text> output, Reporter reporter)
        throws IOException {
      // A sorted set of string sets.
      TreeSet<TreeSet<String>> all_maximal_cliques = new TreeSet<TreeSet<String>>(new IdSetComparator());
      // Merge all maximal cliques from each vertex.
      while(values.hasNext()){
        SetOfVertexSets set_of_set = values.next();
        for(VertexSet set : set_of_set.getVertexSets()){
          TreeSet<String> temp_id_set = new TreeSet<String>();
          for(Vertex vertex : set.getVertexes()){
            temp_id_set.add(vertex.getId());
          }
          all_maximal_cliques.add(temp_id_set);
        }
      }
      
      StringBuffer result_buf = new StringBuffer();
      result_buf.append("[ ");
      for(TreeSet<String> clique : all_maximal_cliques){
        result_buf.append(clique.toString());
        result_buf.append(", ");
      }
      if(all_maximal_cliques.size()>0){
        result_buf.delete(result_buf.length() - 2, result_buf.length());
      }
      result_buf.append(" ]");
      
      // Collect it.
      output.collect(new Text(ConstantLabels.ALL_MAXIMAL_CLIQUES), new Text(result_buf.toString()));
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
    JobConf conf = new JobConf(context, AllMaximalCliquesGenerate.class);
    conf.setJobName("AllMaximalCliquesGenerate");
    
    conf.setMapOutputKeyClass(Text.class);
    conf.setMapOutputValueClass(SetOfVertexSets.class);
    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(Text.class);
    conf.setMapperClass(MapClass.class);        
    // Combiner is not permitted.
    conf.setReducerClass(ReduceClass.class);
    // makes the file format suitable for machine processing.
    conf.setInputFormat(SequenceFileInputFormat.class);
    // Enable compression.
    conf.setCompressMapOutput(true);
    conf.setMapOutputCompressorClass(GzipCodec.class);
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
  
  public static void main(String[] args){
    try {
      int res = ToolRunner.run(new AllMaximalCliquesGenerate(), args);
      System.exit(res);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
