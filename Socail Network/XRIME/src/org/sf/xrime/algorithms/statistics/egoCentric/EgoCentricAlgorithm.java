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
package org.sf.xrime.algorithms.statistics.egoCentric;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.sf.xrime.ProcessorExecutionException;
import org.sf.xrime.algorithms.GraphAlgorithm;
import org.sf.xrime.model.Graph;
import org.sf.xrime.model.edge.Edge;
import org.sf.xrime.model.vertex.AdjSetVertex;
import org.sf.xrime.model.vertex.LabeledAdjVertex;
import org.sf.xrime.utils.SequenceTempDirMgr;

/**
 * Algorithm to compute density of EgoCentric network. 
 * In SNA, the analysis concerned with the density of links surrounding particular agents is called EgoCentric approach.
 * EgoCentricAlgorithm computes information which would be used to deduce EgoCentric density.
 */
public class EgoCentricAlgorithm extends GraphAlgorithm {
	static final public String egoCentricLabelKey = "xrime.algorithm.EgoCentric.label";

	private JobConf jobConf;
	
	/**
	 * Temporary directory manager.
	 */
	private SequenceTempDirMgr tempDirs = null;

	public SequenceTempDirMgr getTempDirs() {
		return tempDirs;
	}

	public void setTempDirs(SequenceTempDirMgr tempDirs) {
		this.tempDirs = tempDirs;
	}

	public Path getSrcPath() {
		Graph src = context.getSource();
		if (src == null) {
			return null;
		}

		try {
			return src.getPath();
		} catch (IllegalAccessException e) {
			return null;
		}
	}

	public void setSrcPath(Path srcPath) {
		Graph src = context.getSource();
		if (src == null) {
			src = new Graph(Graph.defaultGraph());
			context.setSource(src);
		}
		src.setPath(srcPath);
	}

	public Path getDestPath() {
		Graph dest = context.getDestination();
		if (dest == null) {
			return null;
		}

		try {
			return dest.getPath();
		} catch (IllegalAccessException e) {
			return null;
		}
	}

	public void setDestPath(Path destPath) {
		Graph dest = context.getDestination();
		if (dest == null) {
			dest = new Graph(Graph.defaultGraph());
			context.setDestination(dest);
		}
		dest.setPath(destPath);
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
		try {
			if (jobConf == null) {
				jobConf = new JobConf(context, EgoCentricAlgorithm.class);
			}

			if (tempDirs == null) {
				tempDirs = new SequenceTempDirMgr(context.getDestination().getPath());
			}
			tempDirs.setFileSystem(FileSystem.get(jobConf));

			jobConf.setJobName("EgoCentric");

			// {{ First step, compute A --> <B, C>

			jobConf.setMapperClass(EgoCentricMapper.class);
			jobConf.setReducerClass(EgoCentricReducer.class);

			jobConf.setMapOutputValueClass(AdjSetVertex.class);
			jobConf.setOutputKeyClass(Text.class);
			jobConf.setOutputValueClass(Edge.class);

			FileInputFormat.setInputPaths(jobConf, context.getSource().getPath());
			Path middleResult = tempDirs.getTempDir();
			FileOutputFormat.setOutputPath(jobConf, middleResult);
			
			jobConf.setNumReduceTasks(context.getReducerNum());

			jobConf.setInputFormat(SequenceFileInputFormat.class);
			jobConf.setOutputFormat(SequenceFileOutputFormat.class);

			JobClient.runJob(jobConf);
			// }} First step

			// {{ Second step, add first step result to source graph
			jobConf = new JobConf(context, EgoCentricAlgorithm.class);

			FileInputFormat.setInputPaths(jobConf, context.getSource().getPath());
			FileInputFormat.addInputPath(jobConf, middleResult);
			FileOutputFormat.setOutputPath(jobConf, context.getDestination().getPath());
			
			jobConf.setMapperClass(EgoCentricMergerMapper.class);
			jobConf.setReducerClass(EgoCentricMergerReducer.class);
			
			jobConf.setNumReduceTasks(context.getReducerNum());

			jobConf.setMapOutputValueClass(ObjectWritable.class);
			jobConf.setOutputKeyClass(Text.class);
			jobConf.setOutputValueClass(LabeledAdjVertex.class);
			
			jobConf.setInputFormat(SequenceFileInputFormat.class);
			jobConf.setOutputFormat(SequenceFileOutputFormat.class);

			JobClient.runJob(jobConf);
			// }} Second step, add first step result to source graph
		} catch (IllegalAccessException e) {
			throw new ProcessorExecutionException(e);
		} catch (IOException e) {
			throw new ProcessorExecutionException(e);
		}
	}

	/**
	 * Add a main method for command line invocation.
	 */
	public static void main(String[] args) {
	  try {
      int res = ToolRunner.run(new EgoCentricAlgorithm(), args);
      System.exit(res);
    } catch (Exception e) {
      e.printStackTrace();
    }
	}
}
