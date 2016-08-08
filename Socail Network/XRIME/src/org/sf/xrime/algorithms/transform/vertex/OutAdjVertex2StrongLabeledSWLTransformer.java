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
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

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
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.sf.xrime.ProcessorExecutionException;
import org.sf.xrime.Transformer;
import org.sf.xrime.algorithms.utils.GraphAlgorithmMapReduceBase;
import org.sf.xrime.model.edge.AdjVertexEdge;
import org.sf.xrime.model.vertex.LabeledAdjSetVertex;
import org.sf.xrime.model.vertex.LabeledAdjSetVertexWithTwoHopLabel;
import org.sf.xrime.model.edge.Edge;
import org.sf.xrime.model.vertex.AdjVertex;

import org.sf.xrime.model.vertex.LabeledAdjSetVertexWithTwoHopLabel;
import org.sf.xrime.model.edge.AdjVertexEdgeWithLabel;


/**
 * Transform outgoing adjacency vertexes list into another form. The input
 * should be outgoing adjacency vertexes list. The output
 * AdjSetVertexWithTwoHopLabel represents opposites of bi-conneted incident edges
 * 
 * @author xue
 * @author juwei
 */
public class OutAdjVertex2StrongLabeledSWLTransformer extends Transformer {
	/**
	 * Default constructor.
	 */
	public OutAdjVertex2StrongLabeledSWLTransformer() {
		super();
	}

	/**
	 * Another constructor.
	 * 
	 * @param src
	 * @param dest
	 */
	public OutAdjVertex2StrongLabeledSWLTransformer(Path src, Path dest) {
		super(src, dest);
	}
	/**
	 * Transform outgoing adjacency vertexes lists to bi-directed ones.
	 * 
	 *  @author xue
	 */
	  public static class MapClass extends GraphAlgorithmMapReduceBase implements
	    Mapper<Text, AdjVertex, Text, LabeledAdjSetVertex>{
	    @Override
	    public void map(Text key, AdjVertex value,
	        OutputCollector<Text, LabeledAdjSetVertex> output, Reporter reporter)
	        throws IOException {
	      List<Edge> tos = value.getEdges();
	      if(tos.size()==0) return; // This vertex has no outgoing arc.
	      
	      // For the remote end of each arc.
	      for(int i=0; i<tos.size(); i++){
	        String to = tos.get(i).getTo();
	        // Notify the neighbor about myself.
	        LabeledAdjSetVertex notifier = new LabeledAdjSetVertex();
	        notifier.setId(to);
	        notifier.setStringLabel("POTENTIAL_NEIGHBOR", key.toString());
	        //output.collect(arg0, arg1)
	        output.collect(new Text(to), notifier);
	      }
	      // Make myself shown in reducer.
	      LabeledAdjSetVertex myself = new LabeledAdjSetVertex();
	      myself.fromAdjVertexTos(value);
	      output.collect(key, myself);
	    }
	  }
	  
	  /**
	   * Continue to transform the outgoing adjacent vertexes list to bi-directed
	   * ones, and set appropriate label each vertex.
	   * 
	   * @author xue
	   * @author juwei
	   */

	  public static class ReduceClass extends GraphAlgorithmMapReduceBase implements
	    Reducer<Text, LabeledAdjSetVertex, Text, LabeledAdjSetVertexWithTwoHopLabel>{

	    @Override
	    public void reduce(Text key, Iterator<LabeledAdjSetVertex> values,
	        OutputCollector<Text, LabeledAdjSetVertexWithTwoHopLabel> output, Reporter reporter)
	        throws IOException {
	      HashSet<String> potential_neighbors = new HashSet<String>();
	      HashSet<AdjVertexEdge> neighbors = new HashSet<AdjVertexEdge>();
	      while(values.hasNext()){
	        LabeledAdjSetVertex curr_vertex = values.next();
	        if(curr_vertex.getStringLabel("POTENTIAL_NEIGHBOR")==null){
	          // No need to do deep clone here.
	          neighbors.addAll(curr_vertex.getOpposites());
	        }else{
	          // Accumulate potential neighbors.
	          potential_neighbors.add(curr_vertex.getStringLabel("POTENTIAL_NEIGHBOR"));
	        }
	      }
	      for(Iterator<AdjVertexEdge> iterator = neighbors.iterator(); iterator.hasNext();){
	        if(potential_neighbors.contains(iterator.next().getOpposite())){
	          // This is a neighbor we care about.
	        }else{
	          // This is not a neighbor we care about, remove it from the set.
	          iterator.remove();
	        }
	      }
	      if(neighbors.size()==0) return; // This vertex has no incoming arcs corresponding to outgoing
	                                      // arcs.
	      
	      LabeledAdjSetVertexWithTwoHopLabel result = new LabeledAdjSetVertexWithTwoHopLabel();
	      result.setId(key.toString());
	      for (AdjVertexEdge nb: neighbors){
	    	  result.addNeighbor(new AdjVertexEdgeWithLabel(nb.getOpposite()));
	      }
	      
/*	      LabeledAdjSetVertex result = new LabeledAdjSetVertex();
	      result.setId(key.toString());
	      result.setOpposites(neighbors);*/
	      
	      // Collect this.
	      output.collect(key, result);
	    }
	  }

	@Override
	public void execute() throws ProcessorExecutionException {
		// Create a JobConf with default settings.
		JobConf jobConf = new JobConf(conf,
				OutAdjVertex2StrongLabeledSWLTransformer.class);
		jobConf.setJobName("OutAdjVertex2StrongLabeledSWLTransformer");

		jobConf.setMapOutputKeyClass(Text.class);
		jobConf.setMapOutputValueClass(LabeledAdjSetVertex.class);
				
		jobConf.setOutputKeyClass(Text.class);
		jobConf.setOutputValueClass(LabeledAdjSetVertexWithTwoHopLabel.class);
		
		jobConf.setMapperClass(MapClass.class);
		jobConf.setReducerClass(ReduceClass.class);

		// makes the file format suitable for machine processing.
		jobConf.setInputFormat(SequenceFileInputFormat.class);
		jobConf.setOutputFormat(SequenceFileOutputFormat.class);

		// Enable compression.
		jobConf.setCompressMapOutput(true);
		jobConf.setMapOutputCompressorClass(GzipCodec.class);

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

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		try {
			int res = ToolRunner.run(
					new OutAdjVertex2StrongLabeledSWLTransformer(), args);
			System.exit(res);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
}
