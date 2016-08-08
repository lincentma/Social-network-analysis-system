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

import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.sf.xrime.model.Graph;


/**
 * The execution context of a graph algorithm. It wraps all necessary settings
 * of the execution.
 */
public class GraphAlgorithmContext extends Configuration{
	/** Settings of the source graph. */
	protected Graph source;

	/** Settings of the destination graph. */
	protected Graph destination;

	/**
	 * Postfixes are used to distinguish settings for source graph, destination
	 * graph, and this execution of the graph algorithm.
	 */
	static final protected String sourceGraphPostfix = ".s.r.c.";
	static final protected String destinationGraphPostfix = ".d.e.s.t.";

	/**
	 * These strings are used as keys to identify the proposed number of mappers
	 * and reducers.
	 */
	static final protected String mapperNumberKey = "org.sf.xrime.algorithms.mappernum";
	static final protected String reducerNumberKey = "org.sf.xrime.algorithms.reducernum";
	static final protected int mapperDefaultNumber = 6;
	static final protected int reducerDefaultNumber = 6;

	/**
	 * Default constructor.
	 */
	public GraphAlgorithmContext() {
	  super();
	}

	/**
	 * Yet another constructor. Use hadoop configuration to initialize this object.
	 * @param conf configuration
	 */
	public GraphAlgorithmContext(Configuration conf) {
		this(conf, false);
	}
	/**
	 * Yet another constructor. Use hadoop configuration to initialize this object.	 * @param conf the hadoop configuration object.
	 * @param loadGraphs do we need to load src and dest graph from conf.
	 */
	public GraphAlgorithmContext(Configuration conf, boolean loadGraphs){
		super(conf);
		if(loadGraphs) loadGraphs();
	}
	
	/**
	 * Copy constructor. 
	 * @param context
	 */
	public GraphAlgorithmContext(GraphAlgorithmContext context){
	  super(context);
	  loadGraphs();
	}

	/**
	 * Get the source/input graph
	 * @return the source graph of this execution context.
	 */
	public Graph getSource() {
		return source;
	}

	/**
	 * Get the source graph, or the default graph object, if the source graph is null.
	 * Default graph in x-rime is directed simple graph.
	 * @return source graph or default graph.
	 */
	public Graph getSourceOrDefault() {
		if (source != null) {
			return source;
		}

		return Graph.defaultGraph();
	}

	/**
	 * Set the source graph of this execution context.
	 * @param source source graph.
	 */
	public void setSource(Graph source) {
		this.source = source;
		
		// Temp configuration for copy, since we need to clear the entries for source graph, 
		// and Configuration does not provide remove method.
		Configuration temp_conf = new Configuration(this);
		Iterator<Map.Entry<String, String>> iterator = temp_conf.iterator();
		// Clear this context.
		clear();
		// Deal with each entry.
		while (iterator.hasNext()) {
			Map.Entry<String, String> entry = iterator.next();
			String key = entry.getKey();
			if (key.endsWith(sourceGraphPostfix)) {
			  // Skip this entry.
			  continue;
			}else{
			  // Copy this entry.
			  this.set(entry.getKey(), entry.getValue());
			}
		}		
		storeGraph(source, this, sourceGraphPostfix);
	}

	/**
	 * Get the destination/output graph
	 * @return the destination graph of this execution context.
	 */
	public Graph getDestination() {
		return destination;
	}

	/**
	 * Get the destination graph of this execution context, or the default graph if the destination is null.
	 * Default graph in x-rime is directed simple graph.
	 * @return  destination graph or default graph.
	 */
	public Graph getDestinationOrDefault() {
		if (destination != null) {
			return destination;
		}

		return Graph.defaultGraph();
	}

	/**
	 * Set the destination graph of this execution context.
	 * @param destination the destination graph.
	 */
	public void setDestination(Graph destination) {
		this.destination = destination;
		
		// Temp configuration for copy, since we need to clear the entries for source graph, 
		// and Configuration does not provide remove method.
		Configuration temp_conf = new Configuration(this);
		Iterator<Map.Entry<String, String>> iterator = temp_conf.iterator();
		// Clear this context.
		clear();
		// Deal with each entry.
		while (iterator.hasNext()) {
			Map.Entry<String, String> entry = iterator.next();
			String key = entry.getKey();
			if (key.endsWith(destinationGraphPostfix)) {
			  // Skip this entry.
			  continue;
			}else{
			  // Copy this entry.
			  this.set(entry.getKey(), entry.getValue());
			}
		}		
		storeGraph(destination, this, destinationGraphPostfix);
	}

	/**
	 * Get the value of specified parameter.
	 * @param name parameter name.
	 * @return parameter value.
	 */
	public String getParameter(String name) {
		return get(name);
	}

	/**
	 * Update the value of specified parameter.
	 * @param name parameter name.
	 * @param value parameter value.
	 */
	public void setParameter(String name, String value) {
		set(name, value);
	}

	/**
	 * Clear all parameters from settings.
	 */
	public void clearParameters() {
	  clear();
	  storeGraphs();
	}

	/**
	 * Set the proposed number of mapper tasks.
	 * @param num the number.
	 */
	public void setMapperNum(int num) {
		setInt(mapperNumberKey, num);
	}

	/**
	 * Get the proposed number of mapper tasks.
	 * @return the number.
	 */
	public int getMapperNum() {
		return getInt(mapperNumberKey, mapperDefaultNumber);
	}

	/**
	 * Set the proposed number of reducer tasks.
	 * @param num the number.
	 */
	public void setReducerNum(int num) {
		setInt(reducerNumberKey, num);
	}

	/**
	 * Get the proposed number of reducer tasks.
	 * @return the number.
	 */
	public int getReducerNum() {
		return getInt(reducerNumberKey, reducerDefaultNumber);
	}

	/**
	 * Write the settings of source and destination graphs to the embedded hadoop
	 * configuration object.
	 */
	private void storeGraphs() {
		if (source == null) {
			storeGraph(Graph.defaultGraph(), this, sourceGraphPostfix);
		} else {
			storeGraph(source, this, sourceGraphPostfix);
		}

		if (destination == null) {
			storeGraph(Graph.defaultGraph(), this, destinationGraphPostfix);
		} else {
			storeGraph(destination, this, destinationGraphPostfix);
		}
	}

	/**
	 * Initialize the settings of source and destination graphs, with the content
	 * of embedded hadoop configuration object.
	 */
	private void loadGraphs() {
		source = new Graph();
		destination = new Graph();
		loadGraph(source, this , sourceGraphPostfix);
		loadGraph(destination, this, destinationGraphPostfix);
	}

	/**
	 * Write the settings of specified graph to the specified hadoop configuration object, 
	 * using specified postfix to distinguish with other settings.
	 * @param graph specified graph.
	 * @param conf  hadoop configuration object to write to.
	 * @param postfix postfix for this specified graph.
	 */
	private static void storeGraph(Graph graph, Configuration conf, String postfix) {
		if (graph == null || conf == null) {
			return;
		}
		writeProperties(graph.getProperties(), conf, postfix);
	}

	/**
	 * Read the settings for specified graph from the specified hadoop configuration
	 * object. The settings are distinguished from others using specified postfix in
	 * the configuration parameter name.
	 * @param graph specified graph.
	 * @param conf hadoop configuration object to load from.
	 * @param postfix postfix for this specified graph.
	 */
	private static void loadGraph(Graph graph, Configuration conf, String postfix) {
		if (graph == null || conf == null) {
			return;
		}
		readProperties(graph.getProperties(), conf, postfix);
		graph.setProperties(graph.getProperties()); // we should update Graph's
													                      // internal property
	}

	/**
	 * Write settings (properties) to a hadoop configuration object, with specified postfix
	 * to distinguish among other settings.
	 * @param properties properties to write from.
	 * @param conf hadoop configuration object to write to.
	 * @param postfix postfix to distinguish among others.
	 */
	private static void writeProperties(Properties properties,
			Configuration conf, String postfix) {
		if (postfix == null || conf == null || properties == null) {
			return;
		}

		for (String name : properties.stringPropertyNames()) {
			conf.set(name + postfix, properties.getProperty(name));
		}
	}

	/**
	 * Read settings, which are identified with specified postfix, from specified hadoop
	 * configuration object, and store them into specified properties.
	 * @param properties properties to write to.
	 * @param conf hadoop configuration object to read from.
	 * @param postfix postfix to identify settings involved.
	 */ 
	private static void readProperties(Properties properties,
			Configuration conf, String postfix) {
		if (postfix == null || conf == null || properties == null) {
			return;
		}

		Iterator<Map.Entry<String, String>> iterator = conf.iterator();
		
		while (iterator.hasNext()) {
			Map.Entry<String, String> entry = iterator.next();
			String key = entry.getKey();
			if (key.endsWith(postfix)) {
				properties.setProperty(key.substring(0, key.length()
						- postfix.length()), entry.getValue());
			}
		}
	}
}
