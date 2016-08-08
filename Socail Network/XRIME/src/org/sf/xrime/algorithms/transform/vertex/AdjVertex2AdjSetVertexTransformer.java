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
import org.sf.xrime.Transformer;
import org.sf.xrime.model.edge.Edge;
import org.sf.xrime.model.vertex.AdjSetVertex;
import org.sf.xrime.model.vertex.Vertex;
import org.sf.xrime.utils.KeyValuePair;


public class AdjVertex2AdjSetVertexTransformer extends Transformer {
	static final public String edgeFilterKey = "xrime.transformer.edgepredicate";

	/**
	 * The predicate for reduce.
	 */
	protected Class<? extends EdgeFilter> edgeFilter=EverythingEdgeFilter.class;
	
	/**
	 * Default constructor.
	 */
	public AdjVertex2AdjSetVertexTransformer(){
		super();
	}
	
	/**
	 * Normal constructor.
	 * @param src date source dir
	 * @param dest date destination dir
	 */
	public AdjVertex2AdjSetVertexTransformer(Path src, Path dest) {
		super(src, dest);
	}
	
	public Class<? extends EdgeFilter> getEdgeFilter() {
		return edgeFilter;
	}

	public void setEdgeFilter(Class<? extends EdgeFilter> edgeFilter) {
		this.edgeFilter = edgeFilter;
	}
	
	@Override
	public void execute() throws ProcessorExecutionException {
		JobConf jobConf = new JobConf(conf, Vertex2LabeledTransformer.class);
		jobConf.setJobName("Vertex2Labelled");

		jobConf.setMapperClass(AdjVertex2AdjSetVertexMapper.class);
		jobConf.setMapOutputKeyClass(Text.class);
		jobConf.setMapOutputValueClass(ObjectWritable.class);
		jobConf.setNumReduceTasks(reducerNum);
		jobConf.setReducerClass(AdjVertex2AdjSetVertexReducer.class);
		jobConf.setOutputKeyClass(Text.class);
		jobConf.setOutputValueClass(AdjSetVertex.class);
		jobConf.setInputFormat(SequenceFileInputFormat.class);
		jobConf.setOutputFormat(SequenceFileOutputFormat.class);
		jobConf.setClass(edgeFilterKey, edgeFilter, EdgeFilter.class);
		FileInputFormat.setInputPaths(jobConf, srcPath);
		FileOutputFormat.setOutputPath(jobConf, destPath);        

		try {
			this.runningJob = JobClient.runJob(jobConf);
		} catch (IOException e) {
			throw new ProcessorExecutionException(e);
		}		
	}


	/**
	 * The Predicate is an interface that applications can implement to define the filter 
	 * whether the edge should be add to vertex when convert between different Vertexs. 
	 * @author Cai Bin
	 */
	public interface EdgeFilter {
		/**
		 * To fetch <String, Edge> pairs for emit.
		 * @param vertex Input vertex
		 * @param edge   Input edge
		 * @return true, add edge to vertex, else, skip input edge.
		 */
		public List<KeyValuePair<String, Edge>> mapFilter(Vertex vertex, Edge edge);
	}
	
	/**
	 * PredicateTrue return true without any judge.
	 * @author Cai Bin
	 */
	static public class EverythingEdgeFilter implements EdgeFilter {
		/**
		 * Do nothing, trivial constructor.
		 */
		public EverythingEdgeFilter() {			
		}
		
		@Override
		public List<KeyValuePair<String, Edge>> mapFilter(Vertex vertex, Edge edge) {
			ArrayList<KeyValuePair<String, Edge>> ret=new ArrayList<KeyValuePair<String, Edge>>();
			
			ret.add(new KeyValuePair<String, Edge>(edge.getFrom(), edge));
			ret.add(new KeyValuePair<String, Edge>(edge.getTo(), edge));
			ret.add(new KeyValuePair<String, Edge>(vertex.getId(), edge));
			
			return ret;
		}
	}
	
	/**
	 * NPlusEdgeFilter filters that input vertex is the source of input edge.
	 * In graph theory, it predicates N+(v)
	 * @author Cai Bin
	 */
	static public class NPlusEdgeFilter implements EdgeFilter {
		/**
		 * Do nothing, trivial constructor.
		 */
		public NPlusEdgeFilter() {			
		}
		
		@Override
		public List<KeyValuePair<String, Edge>> mapFilter(Vertex vertex, Edge edge) {
			ArrayList<KeyValuePair<String, Edge>> ret=new ArrayList<KeyValuePair<String, Edge>>();
			
			ret.add(new KeyValuePair<String, Edge>(edge.getFrom(), edge));
			
			return ret;
		}		
	}
	
	/**
	 * PredicateNMinus predicates that input vertex is the destination of input edge.
	 * In graph theory, it predicates N-(v)
	 * @author Cai Bin
	 */
	static public class NMinusEdgeFilter implements EdgeFilter {
		/**
		 * Do nothing, trivial constructor.
		 */
		public NMinusEdgeFilter() {			
		}
		
		/* (non-Javadoc)
		 * @see org.sf.xrime.algorithms.transform.vertex.EdgePredicate#evaluate(org.sf.xrime.model.vertex.Vertex, org.sf.xrime.model.edge.Edge)
		 */
		@Override
		public List<KeyValuePair<String, Edge>> mapFilter(Vertex vertex, Edge edge) {
			ArrayList<KeyValuePair<String, Edge>> ret=new ArrayList<KeyValuePair<String, Edge>>();
			
			ret.add(new KeyValuePair<String, Edge>(edge.getTo(), edge));
			
			return ret;
		}
	}
	
	/**
	 * PredicateN = PredicateNPlus || PredicateNMinus.
	 * In graph theory, it predicates N(v), which is the union of N+(v) and N-(v).
	 * @author Cai Bin
	 */
	static public class NEdgeFilter implements EdgeFilter {
		/**
		 * Do nothing, trivial constructor.
		 */
		public NEdgeFilter() {
		}
		
		@Override
		public List<KeyValuePair<String, Edge>> mapFilter(Vertex vertex, Edge edge) {
            ArrayList<KeyValuePair<String, Edge>> ret=new ArrayList<KeyValuePair<String, Edge>>();
			
			ret.add(new KeyValuePair<String, Edge>(edge.getFrom(), edge));
			ret.add(new KeyValuePair<String, Edge>(edge.getTo(), edge));
			
			return ret;			
		}
	}
	
	@Override
  public void setArguments(String[] params) throws ProcessorExecutionException {
		Class<? extends EdgeFilter> edgeFilter=EverythingEdgeFilter.class;
		List<String> other_args = new ArrayList<String>();
		for(int i=0; i < params.length; ++i) {
			try {
				if ("-f".equals(params[i])) {
					String opt=params[++i];
					if(opt.compareToIgnoreCase("src")==0) {
						edgeFilter=NMinusEdgeFilter.class;
					} else if(opt.compareToIgnoreCase("dest")==0) {
						edgeFilter=NPlusEdgeFilter.class;
					} else if(opt.compareToIgnoreCase("both")==0) {
						edgeFilter=NEdgeFilter.class;
					}					
				} else {
					other_args.add(params[i]);
				}
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
		setEdgeFilter(edgeFilter);
  }

  /**
	 * @param args
	 */
	public static void main(String[] args) {
	  try {
      int res = ToolRunner.run(new AdjVertex2AdjSetVertexTransformer(), args);
      System.exit(res);
    } catch (Exception e) {
      e.printStackTrace();
    }
	}
}
