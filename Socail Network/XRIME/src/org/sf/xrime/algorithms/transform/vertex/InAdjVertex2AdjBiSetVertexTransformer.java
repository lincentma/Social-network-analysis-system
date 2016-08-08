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
import org.sf.xrime.model.vertex.AdjBiSetVertex;
import org.sf.xrime.model.vertex.AdjVertex;

/**
 * Transform inbound adjacent vertexes lists into the data structure suitable
 * for some algorithms. The output AdjBiSetVertex collects the opposites of
 * incoming and outgoing incident edges into 2 sets.
 * 
 * @author xue
 */
public class InAdjVertex2AdjBiSetVertexTransformer extends Transformer {
	/**
	 * Default constructor.
	 */
	public InAdjVertex2AdjBiSetVertexTransformer() {
		super();
	}

	/**
	 * Another constructor.
	 * 
	 * @param src
	 * @param dest
	 */
	public InAdjVertex2AdjBiSetVertexTransformer(Path src, Path dest) {
		super(src, dest);
	}

	/**
	 * Map inbound adjacent vertexes lists into AdjBiSetVertex.
	 * 
	 * @author xue
	 */
	public static class MapClass extends MapReduceBase implements
			Mapper<Text, AdjVertex, Text, AdjBiSetVertex> {

		@Override
		public void map(Text key, AdjVertex value,
				OutputCollector<Text, AdjBiSetVertex> output, Reporter reporter)
				throws IOException {
			// The vertex of this inbound adjacent vertexes list.
			AdjBiSetVertex the_vertex = new AdjBiSetVertex();
			String the_id = value.getId();
			// Set the id of the vertex.
			the_vertex.setId(the_id);
			// For each inbound edge of the vertex.
			for (Edge edge : value.getEdges()) {
				String from_vertex = edge.getFrom();
				// Add the other end of the inbound edge to backward vertex set
				// of the vertex.
				the_vertex.addBackwardVertex(new AdjVertexEdge(from_vertex));
				// Create new object for the other end of the inbound edge.
				AdjBiSetVertex new_vertex = new AdjBiSetVertex();
				new_vertex.setId(from_vertex);
				new_vertex.addForwardVertex(new AdjVertexEdge(the_id));
				// Collect this one.
				output.collect(new Text(from_vertex), new_vertex);
			}
			// Collect the vertex.
			output.collect(new Text(the_id), the_vertex);
		}
	}

	/**
	 * Merge AdjBiSetVertex.
	 * 
	 * @author xue
	 */
	public static class ReduceClass extends MapReduceBase implements
			Reducer<Text, AdjBiSetVertex, Text, AdjBiSetVertex> {

		@Override
		public void reduce(Text key, Iterator<AdjBiSetVertex> values,
				OutputCollector<Text, AdjBiSetVertex> output, Reporter reporter)
				throws IOException {
			// Create a new object to accommodate.
			AdjBiSetVertex result_vertex = new AdjBiSetVertex();
			result_vertex.setId(key.toString());
			// Merge forward and backward adjacent vertex sets respectively.
			while (values.hasNext()) {
				AdjBiSetVertex curr_vertex = values.next();
				for (AdjVertexEdge forward_vertex : curr_vertex
						.getForwardVertexes()) {
					result_vertex.addForwardVertex(forward_vertex);
				}
				for (AdjVertexEdge backward_vertex : curr_vertex
						.getBackwardVertexes()) {
					result_vertex.addBackwardVertex(backward_vertex);
				}
			}
			output.collect(key, result_vertex);
		}
	}

	@Override
	public void execute() throws ProcessorExecutionException {
		JobConf jobConf = new JobConf(conf,
				InAdjVertex2AdjBiSetVertexTransformer.class);
		jobConf.setJobName("InAdjVertex2AdjBiSetVertexTransformer");

		// the keys are author names (strings)
		jobConf.setOutputKeyClass(Text.class);
		// the values are adjacent vertexes (Writable)
		jobConf.setOutputValueClass(AdjBiSetVertex.class);
		jobConf.setMapperClass(MapClass.class);
		// No combiner is permitted.
		jobConf.setReducerClass(ReduceClass.class);
		// makes the file format suitable for machine processing.
		jobConf.setInputFormat(SequenceFileInputFormat.class);
		jobConf.setOutputFormat(SequenceFileOutputFormat.class);
		FileInputFormat.setInputPaths(jobConf, srcPath);
		FileOutputFormat.setOutputPath(jobConf, destPath);
		jobConf.setNumMapTasks(mapperNum);
		jobConf.setNumReduceTasks(reducerNum);
		jobConf.setMapOutputCompressorClass(GzipCodec.class);
		jobConf.setCompressMapOutput(true);

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
      int result = ToolRunner.run(new InAdjVertex2AdjBiSetVertexTransformer(), args);
      System.exit(result);
    } catch (Exception e) {
      e.printStackTrace();
    }
	}
}
