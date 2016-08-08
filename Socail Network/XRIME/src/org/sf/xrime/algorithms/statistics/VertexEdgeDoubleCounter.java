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
package org.sf.xrime.algorithms.statistics;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.ToolRunner;
import org.sf.xrime.ProcessorExecutionException;
import org.sf.xrime.algorithms.GraphAlgorithm;
import org.sf.xrime.algorithms.utils.GraphAlgorithmMapReduceBase;
import org.sf.xrime.model.Graph;
import org.sf.xrime.model.vertex.Vertex;


public class VertexEdgeDoubleCounter extends GraphAlgorithm {
	static final public String counterFilterKey = "xrime.algorithms.counterFilter";
		
	private Map<String, Double> counters;
	
	private Class<? extends DoubleCounterFilter> doubleCounterFilterClass=null;
	
	public static double hubsummer;
	public static double authoritysummer;
	
	public Double getDoubleCounter(String name) {
		if(counters==null) {
			return null;
		}
		
		return counters.get(name);
	}

	public double getCounter(String name) {
		Double doubleValue=getDoubleCounter(name);
		if(doubleValue==null) {
			return 0.0;
		}

		return doubleValue.doubleValue();
	}
	
	private void addCounter(String name, double value) {
		if(counters==null) {
			counters=new HashMap<String, Double>();
		}
		
		counters.put(name, new Double(value));
	}

	public Class<? extends DoubleCounterFilter> getDoubleCounterFilterClass() {
		return doubleCounterFilterClass;
	}

	public void setDoubleCounterFilterClass(
			Class<? extends DoubleCounterFilter> counterFilterClass) {
		this.doubleCounterFilterClass = counterFilterClass;
	}
	
	/**
	 * Default constructor.
	 */
	public VertexEdgeDoubleCounter(){
		super();
	}

	/**
	 * Emit the edge number of each vertex.
	 */
	public static class MapClass extends GraphAlgorithmMapReduceBase
	    implements Mapper<Text, Vertex, Text, DoubleWritable> {
		
		private DoubleCounterFilter counterFilter=null;
		
		public void map(Text key, Vertex value, 
				OutputCollector<Text, DoubleWritable> output, 
				Reporter reporter) throws IOException {
			if(counterFilter!=null) {
			    counterFilter.emit(value, output);
			}
		}
		
	    public void configure(JobConf job)
	    {
			super.configure(job);
			
			Class<? extends DoubleCounterFilter> 
			    counterFilterClass=job.getClass(counterFilterKey, 
			    		                        null, 
			    		                        DoubleCounterFilter.class);
			try {
				counterFilter=counterFilterClass.newInstance();
			} catch (InstantiationException e) {
				e.printStackTrace();
			} catch (IllegalAccessException e) {
				e.printStackTrace();
			}
			
			if(counterFilter!=null) {
				counterFilter.configure(job);
			}
	    }
	    
	    public void close()
	    {
			if(counterFilter!=null) {
				counterFilter.close();
			}
	    }
	}

	/**
	 * Calculate the vertex and edge count.
	 */
	public static class ReduceClass extends GraphAlgorithmMapReduceBase
	    implements Reducer<Text, DoubleWritable, Text, DoubleWritable> {

		public void reduce(Text key, Iterator<DoubleWritable> values,
				OutputCollector<Text, DoubleWritable> output, 
				Reporter reporter) throws IOException {
			double sum = 0;
			
			while (values.hasNext()) {
				sum += values.next().get();
			}

			output.collect(key, new DoubleWritable(sum));
		}
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
			JobConf conf = new JobConf(context, AverageVertexDegree.class);
			conf.setJobName("AverageDegree");
	
			conf.setOutputKeyClass(Text.class);
			conf.setOutputValueClass(DoubleWritable.class);
			conf.setMapperClass(MapClass.class);
			conf.setCombinerClass(ReduceClass.class);
			conf.setReducerClass(ReduceClass.class);
			// The format of input data is generated with WritableSerialization.
			conf.setInputFormat(SequenceFileInputFormat.class);
			conf.setOutputFormat(SequenceFileOutputFormat.class);
			FileInputFormat.setInputPaths(conf, getSource().getPath());
			FileOutputFormat.setOutputPath(conf, getDestination().getPath());
			conf.setNumMapTasks(getMapperNum());
			// Only one reducer is permitted, or the return value will be wrong.
			conf.setNumReduceTasks(1);
			
			// Set the possible CounterFilter class
			if(doubleCounterFilterClass!=null) {
				conf.setClass(counterFilterKey, doubleCounterFilterClass, DoubleCounterFilter.class);
			}
	
			this.runningJob = JobClient.runJob(conf);
			
			FileSystem fs = getDestination().getPath().getFileSystem(conf);
			
			Path dataPath=new Path(getDestination().getPath().toString()+"/part-00000");
			SequenceFile.Reader reader=new SequenceFile.Reader(fs, dataPath, conf);

			Writable key = ReflectionUtils.newInstance(reader.getKeyClass().asSubclass(WritableComparable.class), conf);
			Writable value = ReflectionUtils.newInstance(reader.getValueClass().asSubclass(Writable.class), conf);
			
			while ( reader.next(key, value) ) {
				addCounter(((Text) key).toString(), ((DoubleWritable)value).get());
			}
			
			reader.close();
		} catch (IOException e) {
			throw new ProcessorExecutionException(e);
		} catch (IllegalAccessException e) {
			throw new ProcessorExecutionException(e);
		}
	}
	
	static public abstract class DoubleCounterFilter {
	  public abstract void emit(Vertex value, OutputCollector<Text, DoubleWritable> output) throws IOException ;    
	    
	  public void configure(JobConf job)
	  {
	    // do nothing
	  }
	    
	  public void close()
	  {
	    // do nothing
	  }
	}
	
	public static void main(String[] args){
	  try {
      int res = ToolRunner.run(new VertexEdgeDoubleCounter(), args);
      System.exit(res);
    } catch (Exception e) {
      e.printStackTrace();
    }
	}
}