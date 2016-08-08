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
package org.sf.xrime.preprocessing.smth;

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


public class SmthTransformer extends Transformer{
	public SmthTransformer() {
	  super();
	}
	
	public SmthTransformer(Path path_in, Path path_out) {
		super(path_in, path_out);
	}

	@Override
	public void execute() throws ProcessorExecutionException {
		JobConf smthPreprocess = new JobConf(conf, SmthTransformer.class);
		smthPreprocess.setJobName("SmthPreprocess");

		smthPreprocess.setOutputFormat(SequenceFileOutputFormat.class);

		smthPreprocess.setMapperClass(SmthMapper.class);
		smthPreprocess.setReducerClass(SmthReducer.class);
		smthPreprocess.setNumMapTasks(mapperNum);
		smthPreprocess.setNumReduceTasks(reducerNum);

		smthPreprocess.setMapOutputValueClass(AdjVertex.class);
		smthPreprocess.setOutputKeyClass(Text.class);
		smthPreprocess.setOutputValueClass(AdjVertex.class);

		FileInputFormat.setInputPaths(smthPreprocess, srcPath);
		FileOutputFormat.setOutputPath(smthPreprocess, destPath);       

		try {
			this.runningJob = JobClient.runJob(smthPreprocess);
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
      int res = ToolRunner.run(new SmthTransformer(), args);
      System.exit(res);
    } catch (Exception e) {
      e.printStackTrace();
    }
	}
}
