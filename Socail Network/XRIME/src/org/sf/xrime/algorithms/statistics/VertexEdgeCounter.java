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
import org.apache.hadoop.io.LongWritable;
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
import org.sf.xrime.model.vertex.AdjBiSetVertex;
import org.sf.xrime.model.vertex.AdjSetVertex;
import org.sf.xrime.model.vertex.AdjVertex;
import org.sf.xrime.model.vertex.Vertex;


/**
 * Calculate the number of vertex and edge in the graph.
 */
public class VertexEdgeCounter extends GraphAlgorithm {
	static final public String counterFilterKey = "xrime.algorithms.counterFilter";
		
	private Map<String, Long> counters;
	
	private Class<? extends CounterFilter> counterFilterClass=null;
	
	public Long getLongCounter(String name) {
		if(counters==null) {
			return null;
		}
		
		return counters.get(name);
	}

	public long getCounter(String name) {
		Long longValue=getLongCounter(name);
		if(longValue==null) {
			return 0;
		}

		return longValue.longValue();
	}
	
	private void addCounter(String name, long value) {
		if(counters==null) {
			counters=new HashMap<String, Long>();
		}
		
		counters.put(name, new Long(value));
	}

	public Class<? extends CounterFilter> getCounterFilterClass() {
		return counterFilterClass;
	}

	public void setCounterFilterClass(
			Class<? extends CounterFilter> counterFilterClass) {
		this.counterFilterClass = counterFilterClass;
	}
	
	/**
	 * Default constructor.
	 */
	public VertexEdgeCounter(){
		super();
	}

	/**
	 * Emit the edge number of each vertex.
	 */
	public static class MapClass extends GraphAlgorithmMapReduceBase
	    implements Mapper<Text, Vertex, Text, LongWritable> {
		
		private CounterFilter counterFilter=null;
		
		public void map(Text key, Vertex value, 
				OutputCollector<Text, LongWritable> output, 
				Reporter reporter) throws IOException {
			counterFilter.emit(value, output);
		}
		
		public void configure(JobConf job)
		{
			super.configure(job);
			
			Class<? extends CounterFilter> 
			    counterFilterClass=job.getClass(counterFilterKey, 
			    		                        GeneralCounterFilter.class, 
			    		                        CounterFilter.class);
			try {
				counterFilter=counterFilterClass.newInstance();
			} catch (InstantiationException e) {
				counterFilter=new GeneralCounterFilter();
				e.printStackTrace();
			} catch (IllegalAccessException e) {
				counterFilter=new GeneralCounterFilter();
				e.printStackTrace();
			}
			
			counterFilter.configure(job);
		}
	    
		public void close()
		{
		  counterFilter.close();
		}
	}

	/**
	 * Calculate the vertex and edge count.
	 */
	public static class ReduceClass extends GraphAlgorithmMapReduceBase
	    implements Reducer<Text, LongWritable, Text, LongWritable> {

		public void reduce(Text key, Iterator<LongWritable> values,
				OutputCollector<Text, LongWritable> output, 
				Reporter reporter) throws IOException {
			long sum = 0;
			
			while (values.hasNext()) {
				sum += values.next().get();
			}

			output.collect(key, new LongWritable(sum));
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
	
			// the keys are a pseudo one ("Average_Degree")
			conf.setOutputKeyClass(Text.class);
			// the values are degrees (ints)
			conf.setOutputValueClass(LongWritable.class);
			conf.setMapperClass(MapClass.class);
			conf.setCombinerClass(ReduceClass.class);
			// No combiner is permitted.
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
			if(counterFilterClass!=null) {
				conf.setClass(counterFilterKey, counterFilterClass, CounterFilter.class);
			}
			
			this.runningJob = JobClient.runJob(conf);
			
			FileSystem fs = getDestination().getPath().getFileSystem(conf);
			Path dataPath=new Path(getDestination().getPath().toString()+"/part-00000");
			SequenceFile.Reader reader=new SequenceFile.Reader(fs, dataPath, conf);

			Writable key = ReflectionUtils.newInstance(reader.getKeyClass().asSubclass(WritableComparable.class), conf);
			Writable value = ReflectionUtils.newInstance(reader.getValueClass().asSubclass(Writable.class), conf);
			
			while ( reader.next(key, value) ) {
				addCounter(((Text) key).toString(), ((LongWritable)value).get());
			}
			
			reader.close();
		} catch (IOException e) {
			throw new ProcessorExecutionException(e);
		} catch (IllegalAccessException e) {
			throw new ProcessorExecutionException(e);
		}
	}
	

	static public abstract class CounterFilter {
	  public abstract void emit(Vertex value, OutputCollector<Text, LongWritable> output) throws IOException ;    
	    
	  public void configure(JobConf job)
	  {
	    // do nothing
	  }
	    
	  public void close()
	  {
	    // do nothing
	  }
	}
	
	static public class GeneralCounterFilter extends CounterFilter {
		static final public String edgeCounterKey   = "edge.counter";
		static final public String vertexCounterKey = "vertex.counter";
		
		Text edgeKey=new Text(edgeCounterKey);
		Text vertexKey=new Text(vertexCounterKey);
		LongWritable one=new  LongWritable(1);
		
		public GeneralCounterFilter() {			
		}

		@Override
		public void emit(Vertex value,
				         OutputCollector<Text, LongWritable> output) throws IOException {
			// Get the degree from the input adjacent vertexes list.
			if(value instanceof AdjVertex) {
				output.collect(edgeKey, 
					new LongWritable( (((AdjVertex)value).getEdges()==null)? 0 : ((AdjVertex)value).getEdges().size()) );
	
			} else if (value instanceof AdjSetVertex) {
				output.collect(edgeKey, 
					new LongWritable( (((AdjSetVertex)value).getOpposites()==null)? 0 : ((AdjSetVertex)value).getOpposites().size()) );
		    } else if (value instanceof AdjBiSetVertex) {
				output.collect(edgeKey, 
					new LongWritable( (((AdjBiSetVertex)value).getBackwardVertexes()==null)? 0 : ((AdjBiSetVertex)value).getBackwardVertexes().size()) );
		    }
				
			output.collect(vertexKey, one);					
		}		
	}

	public static void main(String[] args){
	  try {
      int res = ToolRunner.run(new VertexEdgeCounter(), args);
      System.exit(res);
    } catch (Exception e) {
      e.printStackTrace();
    }
	}
}