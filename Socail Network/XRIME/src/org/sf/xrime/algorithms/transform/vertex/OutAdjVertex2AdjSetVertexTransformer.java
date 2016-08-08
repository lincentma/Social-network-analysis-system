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
import org.sf.xrime.model.edge.AdjVertexEdge;
import org.sf.xrime.model.edge.Edge;
import org.sf.xrime.model.vertex.AdjSetVertex;
import org.sf.xrime.model.vertex.AdjVertex;


/**
 * Transform outgoing adjacency vertexes list into another form. The input should
 * be outgoing adjacency vertexes list. The output AdjSetVertex represents opposites
 * of all incident edges (both incoming and outgoing).
 * @author xue
 */
public class OutAdjVertex2AdjSetVertexTransformer extends Transformer {
	/**
	 * Default constructor.
	 */
	public OutAdjVertex2AdjSetVertexTransformer(){
		super();
	}
	/**
	 * Another constructor.
	 * @param src
	 * @param dest
	 */
	public OutAdjVertex2AdjSetVertexTransformer(Path src, Path dest){
		super(src, dest);
	}
	/**
	 * Transform outgoing adjacency vertexes lists to undirected ones.
	 * @author xue
	 */
	public static class MapClass extends MapReduceBase implements
	Mapper<Text, AdjVertex, Text, AdjSetVertex>{
		@Override
		public void map(Text key, AdjVertex value,
				OutputCollector<Text, AdjSetVertex> output, Reporter reporter)
		throws IOException {
			// The vertex "key" itself.
			AdjSetVertex out_vertex = new AdjSetVertex();
			// Don't forget this!!! 2009-05-10.
			out_vertex.fromAdjVertexTos(value);
			output.collect(key, out_vertex);
			// For each backward edge.
			for(Edge edge : value.getEdges()){
				AdjSetVertex new_vertex = new AdjSetVertex();
				new_vertex.setId(edge.getTo());
				new_vertex.addOpposite(new AdjVertexEdge(value.getId()));
				output.collect(new Text(edge.getTo()), new_vertex);
			}
		}
	}

	/**
	 * Continue to transform the outgoing adjacent vertexes list to undirected ones, and
	 * set appropriate label each vertex.
	 * @author xue
	 */
	public static class ReduceClass extends MapReduceBase implements
	Reducer<Text, AdjSetVertex, Text, AdjSetVertex>{

		@Override
		public void reduce(Text key, Iterator<AdjSetVertex> values,
				OutputCollector<Text, AdjSetVertex> output, Reporter reporter)
		throws IOException {
			AdjSetVertex result_vertex = new AdjSetVertex();
			result_vertex.setId(key.toString());
			// Merge all adjacent vertexes into one set.
			while(values.hasNext()){
				AdjSetVertex curr_set = values.next();
				for(AdjVertexEdge to : curr_set.getOpposites()){
					result_vertex.addOpposite(to);
				}
			}
			output.collect(key, result_vertex);
		}
	}

	@Override
	public void execute() throws ProcessorExecutionException {
		JobConf jobConf =  new JobConf(conf, OutAdjVertex2AdjSetVertexTransformer.class);
		jobConf.setJobName("OutAdjVertex2AdjSetVertexTransformer");

		// the keys are vertex identifiers (strings)
		jobConf.setOutputKeyClass(Text.class);
		// the values are adjacent vertexes with labels (Writable)
		jobConf.setOutputValueClass(AdjSetVertex.class);
		jobConf.setMapperClass(MapClass.class);        
		// no combiner is needed.
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
      int res = ToolRunner.run(new OutAdjVertex2AdjSetVertexTransformer(), args);
      System.exit(res);
    } catch (Exception e) {
      e.printStackTrace();
    }
	}
}
