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
package org.sf.xrime.postprocessing;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.sf.xrime.ProcessorExecutionException;
import org.sf.xrime.Transformer;


/**
 * This class is used to transform all graph algorithms' outputs in form of sequence files
 * into text files, which are human-readable.
 */
public class SequenceFileToTextFileTransformer extends Transformer{
  /**
   * Default constructor.
   */
  public SequenceFileToTextFileTransformer(){
    super();
  }
  /**
   * Constructor.
   * @param src
   * @param dest
   */
	public SequenceFileToTextFileTransformer(Path src, Path dest) {
	  super(src, dest);
	}

  @Override
  public void execute() throws ProcessorExecutionException {
    JobConf convertor =  new JobConf(conf, SequenceFileToTextFileTransformer.class);
    convertor.setJobName("SequenceFileToTextFileTransformer");

    convertor.setInputFormat(SequenceFileInputFormat.class);
    convertor.setOutputFormat(TextOutputFormat.class);
    
	  convertor.setMapperClass(SequenceFileToTextFileMapper.class);
	  convertor.setMapOutputValueClass(Text.class);
    convertor.setOutputKeyClass(Text.class);
    convertor.setOutputValueClass(Text.class);
        
    // ONLY mapper, no combiner, no reducer.
    convertor.setNumMapTasks(getMapperNum());
	  convertor.setNumReduceTasks(0);

    FileInputFormat.setInputPaths(convertor, srcPath);
    FileOutputFormat.setOutputPath(convertor, destPath);		    
    try {
      this.runningJob = JobClient.runJob(convertor);
    } catch (IOException e) {
      throw new ProcessorExecutionException(e);
    }  }
  
  public static void main(String[] args) {
    try {
      int res = ToolRunner.run(new SequenceFileToTextFileTransformer(), args);
      System.exit(res);
    } catch (Exception e) {
      e.printStackTrace();
    }

  }

}
