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
package org.sf.xrime.algorithms;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.util.Tool;
import org.sf.xrime.Processor;
import org.sf.xrime.ProcessorExecutionException;
import org.sf.xrime.model.Graph;


/**
 * An abstract base class for all graph algorithm instances, which are combinations of
 * algorithm (the logic of algorithm) and corresponding execution context (modifiable
 * settings used during the execution). 
 * <p>
 * In general, GraphAlgorithm will take an input graph, which is stored in file system
 * (typically in HDFS) and after invoking method: execute(), an output graph will be 
 * generated and stored in file system.
 */
public abstract class GraphAlgorithm implements Processor, Tool {
	/**
	 * The execution context of algorithm.
	 */
	protected GraphAlgorithmContext context;
	
	/**
	 * Use to retrieve final execution status.
	 */
	protected RunningJob runningJob = null;
	
	/**
	 * Default constructor.
	 */
	public GraphAlgorithm() {
		context = new GraphAlgorithmContext();
	}
	
  @Override
  public Configuration getConf() {
    return context;
  }

  @Override
  public void setConf(Configuration conf) {
    context = new GraphAlgorithmContext(conf);
  }	
	
	/**
	 * Get the execution context of this algorithm instance.
	 * @return the execution context.
	 */
	public GraphAlgorithmContext getContext() {
		return context;
	}
	
	/**
	 * Set the execution context of this algorithm instance.
	 * @param context the execution context.
	 */
	public void setContext(GraphAlgorithmContext context) {
		this.context = new GraphAlgorithmContext(context);
	}
	
	/**
	 * Get the source graph of this algorithm instance.
	 * Since all algorithms are based on Map/Reduce model, the input data should not be changed.
	 * @return input graph.
	 */
	public Graph getSource() {
		return context.getSource();
	}
	
	/**
	 * Set the source graph of this algorithm instance.
	 * @param source input graph.
	 */
	public void setSource(Graph source) {
		context.setSource(source);
	}
	
	/**
	 * Get the destination graph of this algorithm instance.
	 * @return output graph.
	 */
	public Graph getDestination() {
		return context.getDestination();
	}
	
	/**
	 * Set the destination graph of this algorithm instance.
	 * @param destination output graph.
	 */
	public void setDestination(Graph destination) {
		context.setDestination(destination);
	}
	
	/**
	 * Create destination graph by duplicating settings of source graph and
	 * use specified path as the underlying data path of the destination 
	 * graph.
	 * @param path new path to store graph data.
	 */
	public void createDestination(Path path) {
		Graph destination=new Graph(context.getSource().getProperties());
		List<Path> paths=new ArrayList<Path>();
		paths.add(path);
		destination.setPaths(paths);
		context.setDestination(destination);
	}
	
	/**
	 * Get specified parameter used in this algorithm instance.
	 * @param name parameter name.
	 * @return parameter value.
	 */
	public String getParameter(String name) {
		return context.getParameter(name);
	}
	
	/**
	 * Set the specified parameter with specified value.
	 * @param name parameter name.
	 * @param value parameter value.
	 */
	public void setParameter(String name, String value) {
		context.setParameter(name, value);
	}
	
	/**
	 * Clear all parameters of this algorithm instance.
	 */
	public void clearParameters(){
	  context.clearParameters();
	}
	
	/**
	 * Set the proposed number of mapper tasks. 
	 * @param num the proposed number.
	 */
	public void setMapperNum(int num){
	  context.setMapperNum(num);
	}
	
	/**
	 * Get the proposed number of mapper tasks.
	 * @return the proposed number.
	 */
	public int getMapperNum(){
	  return context.getMapperNum();
	}
	
	/**
	 * Set the proposed number of reducer tasks.
	 * @param num the proposed number.
	 */
	public void setReducerNum(int num){
	  context.setReducerNum(num);
	}
	
	/**
	 * Get the proposed number of reducer tasks.
	 * @return the proposed number.
	 */
	public int getReducerNum(){
	  return context.getReducerNum();
	}
	/**
	 * Get the final status of executing this algorithm instance.
	 * @return final status of executing.
	 */
	public RunningJob getFinalStatus(){
	  return runningJob;
	}
	
	
	/**
	 * {@inheritDoc}
	 * <p>Those algorithms which do not deal directly with command line arguments and work as a component
	 * of a larger algorithm, don't need to override this method. Instead, they could inherit settings from
	 * the "larger" algorithm by calling proper setter methods.</p>
	 */
  @Override
	public void setArguments(String[] params) throws ProcessorExecutionException {
  	// Do nothing.
	}

	@Override
  public int run(String[] args) throws Exception {
    List<String> other_args = new ArrayList<String>();
    // Deal with mapper and reducer numbers.
    try{
      for(int i=0; i < args.length; ++i) {
        if ("-m".equals(args[i])) {
          setMapperNum(Integer.parseInt(args[++i]));
        } else if ("-r".equals(args[i])) {
          setReducerNum(Integer.parseInt(args[++i]));
        /**  
        } else if ("-vmr".equals(args[i])) {
          context.setBoolean("mapred.versioned.enabled", true);
        } else if ("-parents".equals(args[i])){
          context.set("mapred.versioned.parents", args[++i]);
        */        } else {
          other_args.add(args[i]);
        }
      }
    }catch(Exception except){
      // Wrap and re-throw.
      throw new ProcessorExecutionException(except);
    }
    // Deal with processor specific parameters.
    setArguments(other_args.toArray(new String[2]));
    // Execute the processor.
    execute();
    return 0;
  }
}
