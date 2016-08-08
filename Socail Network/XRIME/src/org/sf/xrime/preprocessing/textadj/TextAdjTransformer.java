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
package org.sf.xrime.preprocessing.textadj;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.util.ToolRunner;

import org.sf.xrime.ProcessorExecutionException;
import org.sf.xrime.Transformer;
import org.sf.xrime.model.vertex.AdjVertex;
import org.sf.xrime.preprocessing.smth.SmthReducer;


/**
 * TextAdjTransformer used to transform data from adjacency lists in text file into AdjVertex objects.
 * The format of input data is:
 * <source vertex> <destination vertex 1> <destination vertex 2> ... <destination vertex n>
 * Spaces are used to separate fields.
 */
public class TextAdjTransformer extends Transformer{
	public TextAdjTransformer() {
	  super();
	}
	
	public TextAdjTransformer(Path path_in, Path path_out) {
		super(path_in, path_out);
	}

	@Override
	public void execute() throws ProcessorExecutionException {
		JobConf textAdjPreprocess = new JobConf(conf, TextAdjTransformer.class);
		textAdjPreprocess.setJobName("SmthPreprocess");

		textAdjPreprocess.setOutputFormat(SequenceFileOutputFormat.class);

		textAdjPreprocess.setMapperClass(TextAdjMapper.class);
		textAdjPreprocess.setReducerClass(SmthReducer.class);
		textAdjPreprocess.setNumMapTasks(mapperNum);
		textAdjPreprocess.setNumReduceTasks(reducerNum);

		textAdjPreprocess.setMapOutputValueClass(AdjVertex.class);
		textAdjPreprocess.setOutputKeyClass(Text.class);
		textAdjPreprocess.setOutputValueClass(AdjVertex.class);

		FileInputFormat.setInputPaths(textAdjPreprocess, srcPath);
		FileOutputFormat.setOutputPath(textAdjPreprocess, destPath);       

		try {
			this.runningJob = JobClient.runJob(textAdjPreprocess);
		} catch (IOException e) {
			throw new ProcessorExecutionException(e);
		}
	}

	/**
	 * Main method for this transformer.
	 * @param args input arguments
	 */
	public static void main(String[] args) {
	  try {
      int res = ToolRunner.run(new TextAdjTransformer(), args);
      System.exit(res);
    } catch (Exception e) {
      e.printStackTrace();
    }
	}
}
