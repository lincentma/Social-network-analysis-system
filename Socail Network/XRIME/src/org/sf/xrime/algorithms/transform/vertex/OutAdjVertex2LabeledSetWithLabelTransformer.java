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
import org.sf.xrime.model.edge.AdjVertexEdgeWithLabel;
import org.sf.xrime.model.edge.Edge;
import org.sf.xrime.model.vertex.LabeledAdjSetVertexWithTwoHopLabel;
import org.sf.xrime.model.vertex.AdjVertex;

/**
 * Transform outgoing adjacency vertexes list into another form. The input
 * should be outgoing adjacency vertexes list. The output
 * LabeledAdjSetVertexWithTwoHopLabel represents opposites of all incident edges (both
 * incoming and outgoing).
 * 
 * @author xue
 * @author juwei
 */
public class OutAdjVertex2LabeledSetWithLabelTransformer extends Transformer {
	/**
	 * Default constructor.
	 */
	public OutAdjVertex2LabeledSetWithLabelTransformer() {
		super();
	}

	/**
	 * Another constructor.
	 * 
	 * @param src
	 * @param dest
	 */
	public OutAdjVertex2LabeledSetWithLabelTransformer(Path src, Path dest) {
		super(src, dest);
	}
	
	private enum Counter{
		VertexNum;
	}

	/**
	 * Transform outgoing adjacency vertexes lists to undirected ones.
	 * 
	 * @author xue
	 */
	@SuppressWarnings("deprecation")
	public static class MapClass extends MapReduceBase implements
			Mapper<Text, AdjVertex, Text, LabeledAdjSetVertexWithTwoHopLabel> {
		@Override
		public void map(Text key, AdjVertex value,
				OutputCollector<Text, LabeledAdjSetVertexWithTwoHopLabel> output,
				Reporter reporter) throws IOException {
			// The vertex "key" itself.
			LabeledAdjSetVertexWithTwoHopLabel out_vertex = new LabeledAdjSetVertexWithTwoHopLabel();
			// Don't forget this!!! 2009-05-10.
			out_vertex.fromAdjVertexTos(value);
			output.collect(key, out_vertex);
			// For each backward edge.
			for (Edge edge : value.getEdges()) {
				LabeledAdjSetVertexWithTwoHopLabel new_vertex = new LabeledAdjSetVertexWithTwoHopLabel();
				new_vertex.setId(edge.getTo());
				new_vertex
						.addNeighbor(new AdjVertexEdgeWithLabel(value.getId()));
				// new_vertex.addOpposite(new AdjVertexEdge(value.getId()));
				output.collect(new Text(edge.getTo()), new_vertex);
			}
		}
	}

	/**
	 * Continue to transform the outgoing adjacent vertexes list to undirected
	 * ones, and set appropriate label each vertex.
	 * 
	 * @author xue
	 * @author juwei
	 */
	@SuppressWarnings("deprecation")
	public static class ReduceClass extends MapReduceBase
			implements
			Reducer<Text, LabeledAdjSetVertexWithTwoHopLabel, Text, LabeledAdjSetVertexWithTwoHopLabel> {

		@Override
		public void reduce(Text key,
				Iterator<LabeledAdjSetVertexWithTwoHopLabel> values,
				OutputCollector<Text, LabeledAdjSetVertexWithTwoHopLabel> output,
				Reporter reporter) throws IOException {
			LabeledAdjSetVertexWithTwoHopLabel result_vertex = new LabeledAdjSetVertexWithTwoHopLabel();
			result_vertex.setId(key.toString());
			// Merge all adjacent vertexes into one set.
			while (values.hasNext()) {
				LabeledAdjSetVertexWithTwoHopLabel curr_set = values.next();
				for (AdjVertexEdgeWithLabel to : curr_set.getNeighbors()) {
					result_vertex.addNeighbor(to);
				}
			}
			reporter.incrCounter(Counter.VertexNum, 1);
			output.collect(key, result_vertex);
		}
	}

	@SuppressWarnings("deprecation")
	@Override
	public void execute() throws ProcessorExecutionException {
		// Create a JobConf with default settings.
		JobConf jobConf = new JobConf(conf,
				OutAdjVertex2LabeledSetWithLabelTransformer.class);
		jobConf.setJobName("OutAdjVertex2LabeledSetWithLabelTransformer");

		// the keys are vertex identifiers (strings)
		jobConf.setOutputKeyClass(Text.class);
		// the values are adjacent vertexes with labels (Writable)
		jobConf.setOutputValueClass(LabeledAdjSetVertexWithTwoHopLabel.class);

		jobConf.setMapperClass(MapClass.class);
		jobConf.setCombinerClass(ReduceClass.class);
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
			System.out.println("Output Node Num. ="+this.runningJob.getCounters().getCounter(Counter.VertexNum));
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
					new OutAdjVertex2LabeledSetWithLabelTransformer(), args);
			System.exit(res);
		} catch (Exception e) {
			e.printStackTrace();
		}

	}
}
