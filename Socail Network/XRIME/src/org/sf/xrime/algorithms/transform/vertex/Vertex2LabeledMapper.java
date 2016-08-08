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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.sf.xrime.model.label.LabelAdder;
import org.sf.xrime.model.label.Labelable;
import org.sf.xrime.model.vertex.Vertex;
import org.sf.xrime.model.vertex.VertexUtils;


/**
 * Mapper used to transform vertexes to vertexes with labels.
 */
public class Vertex2LabeledMapper extends MapReduceBase 
    implements Mapper<Text, Vertex, Text, Vertex> {

	protected LabelAdder theLabelAdder=null;
	Configuration conf;
	
	@Override
	public void map(Text key, Vertex value,
			OutputCollector<Text, Vertex> output, Reporter reporter)
			throws IOException {
		Vertex vertex=(Vertex) VertexUtils.getLabledVertex(value);
		
		if(theLabelAdder!=null) {
			theLabelAdder.addLabels((Labelable) vertex, vertex);
		}
		
		output.collect(key, vertex);
	}
	
	public void configure(JobConf job) {
		super.configure(job);
		
		conf=job;
		
		Class<? extends LabelAdder> labelAdderClass=job.getClass(Vertex2LabeledTransformer.labelFactoryKey, null, LabelAdder.class);
		try {
		  if(labelAdderClass==null){
		    // Allow null label adder, i.e., allow Labeled objects with empty initial label set.
		    theLabelAdder = null;
		  }else{
		    theLabelAdder = labelAdderClass.newInstance();
		    theLabelAdder.configure(job);
		  }
		} catch (InstantiationException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		}
	}
	
	public void close() throws IOException {
		if(theLabelAdder!=null) {
			theLabelAdder.close();
		}
		super.close();
	}
}
