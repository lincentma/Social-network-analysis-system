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
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.sf.xrime.ProcessorExecutionException;
import org.sf.xrime.Transformer;
import org.sf.xrime.model.label.LabelAdder;
import org.sf.xrime.model.vertex.LabeledAdjBiSetVertex;
import org.sf.xrime.model.vertex.LabeledAdjSetVertex;
import org.sf.xrime.model.vertex.Vertex;


public class Vertex2LabeledTransformer extends Transformer {
	static final public String labelFactoryKey = "xrime.transformer.tolabeled.factory";
		
	/**
	 * The mapper output value class.
	 */
	protected Class<?> theClass=null;

	/**
	 * The class used to add labels to output value.
	 */
	protected Class<? extends LabelAdder> theLabelAdderClass=null;

	/**
	 * Default constructor.
	 */
	public Vertex2LabeledTransformer(){
		super();
	}

	/**
	 * Normal constructor.
	 * @param src date source dir
	 * @param dest date destination dir
	 */
	public Vertex2LabeledTransformer(Path src, Path dest) {
		super(src, dest);
	}

	public void setOutputValueClass(Class<?> theClass) {
		this.theClass=theClass;
	}

	public void setLabelAdderClass(Class<? extends LabelAdder> theLabelAdderClass) {
		this.theLabelAdderClass = theLabelAdderClass;
	}
	
	@Override
  public void setArguments(String[] params) throws ProcessorExecutionException {
		List<String> other_args = new ArrayList<String>();
		for(int i=0; i < params.length; ++i) {
			try {
				if ("-t".equals(params[i])) {
					char option=params[++i].charAt(0);
					if(option=='a') {
						setOutputValueClass(LabeledAdjSetVertex.class);
					} else if (option=='s') {
						setOutputValueClass(LabeledAdjSetVertex.class);
					} if (option=='b') {
						setOutputValueClass(LabeledAdjBiSetVertex.class);
					}					
				} else {
					other_args.add(params[i]);
				}
			} catch (NumberFormatException except) {
			  throw new ProcessorExecutionException(except);
			} catch (ArrayIndexOutOfBoundsException except) {
			  throw new ProcessorExecutionException(except);
			}
		}
		// Make sure there are exactly 2 parameters left.
		if (other_args.size() != 2) {
			throw new ProcessorExecutionException("Wrong number of parameters: " +
					other_args.size() + " instead of 2.");
		}

		setSrcPath(new Path(other_args.get(0)));
		setDestPath(new Path(other_args.get(1)));
  }

  @Override
	public void execute() throws ProcessorExecutionException {
		JobConf jobConf = new JobConf(conf, Vertex2LabeledTransformer.class);
		jobConf.setJobName("Vertex2Labelled");

		jobConf.setMapperClass(Vertex2LabeledMapper.class);
		jobConf.setNumReduceTasks(0);
		jobConf.setOutputKeyClass(Text.class);
		if(this.theClass==null) {
			throw new ProcessorExecutionException("Need to specify the output value class.");
		}
		jobConf.setOutputValueClass(this.theClass);
		jobConf.setMapOutputKeyClass(Text.class);
		jobConf.setMapOutputValueClass(Vertex.class);
		jobConf.setInputFormat(SequenceFileInputFormat.class);
		jobConf.setOutputFormat(SequenceFileOutputFormat.class);
		if(theLabelAdderClass!=null) {
			jobConf.setClass(labelFactoryKey, theLabelAdderClass, LabelAdder.class);
		}
		FileInputFormat.setInputPaths(jobConf, srcPath);
		FileOutputFormat.setOutputPath(jobConf, destPath);        

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
      int res = ToolRunner.run(new Vertex2LabeledTransformer(), args);
      System.exit(res);
    } catch (Exception e) {
      e.printStackTrace();
    }
	}
}
