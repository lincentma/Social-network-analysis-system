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
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.sf.xrime.ProcessorExecutionException;
import org.sf.xrime.Transformer;
import org.sf.xrime.model.edge.AdjVertexEdge;
import org.sf.xrime.model.edge.AdjVertexEdgeWithLabel;
import org.sf.xrime.model.vertex.AdjSetVertex;
import org.sf.xrime.model.vertex.LabeledAdjSetVertexWithTwoHopLabel;

/**
 * Transform LabeledAdjSetVertexWithTwoHopLabel into AdjSetVertex.
 * 
 * @author juwei
 */
public class LabeledSetWithLabel2AdjSetVertexTransformer extends Transformer {
	/**
	 * Default constructor.
	 */
	public LabeledSetWithLabel2AdjSetVertexTransformer() {
		super();
	}

	/**
	 * Another constructor.
	 * 
	 * @param src
	 * @param dest
	 */
	public LabeledSetWithLabel2AdjSetVertexTransformer(Path src, Path dest) {
		super(src, dest);
	}

	/**
	 * Transform AdjSetVertexWithTwoHopLabel into AdjSetVertex
	 * 
	 * @author xue
	 */
	public static class MapClass extends MapReduceBase implements
			Mapper<Text, LabeledAdjSetVertexWithTwoHopLabel, Text, AdjSetVertex> {
		@Override
		public void map(Text key, LabeledAdjSetVertexWithTwoHopLabel value,
				OutputCollector<Text, AdjSetVertex> output, Reporter reporter)
				throws IOException {
			AdjSetVertex outputValue = new AdjSetVertex();
			outputValue.setId(value.getId());
			for (AdjVertexEdgeWithLabel neighbor : value.getNeighbors()) {
				outputValue.addOpposite(new AdjVertexEdge(neighbor
						.getOpposite()));
			}
			output.collect(key, outputValue);
		}
	}


	@Override
	public void execute() throws ProcessorExecutionException {
		JobConf jobConf = new JobConf(conf,
				LabeledSetWithLabel2AdjSetVertexTransformer.class);
		jobConf.setJobName("LabeledSetWithLabel2AdjSetVertexTransformer");

		// the keys are vertex identifiers (strings)
		jobConf.setOutputKeyClass(Text.class);
		// the values are adjacent vertexes with labels (Writable)
		jobConf.setOutputValueClass(AdjSetVertex.class);

		jobConf.setMapperClass(MapClass.class);
		// no combiner is needed.
		// no reduce is needed.
		// jobConf.setReducerClass(ReduceClass.class);

		// makes the file format suitable for machine processing.
		jobConf.setInputFormat(SequenceFileInputFormat.class);
		jobConf.setOutputFormat(SequenceFileOutputFormat.class);

		// Enable compression.
		jobConf.setCompressMapOutput(true);
		jobConf.setMapOutputCompressorClass(GzipCodec.class);

		FileInputFormat.setInputPaths(jobConf, srcPath);
		FileOutputFormat.setOutputPath(jobConf, destPath);

		jobConf.setNumMapTasks(mapperNum);
		jobConf.setNumReduceTasks(0);

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
					new LabeledSetWithLabel2AdjSetVertexTransformer(), args);
			System.exit(res);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
