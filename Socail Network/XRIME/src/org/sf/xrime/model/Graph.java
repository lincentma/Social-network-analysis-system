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
package org.sf.xrime.model;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.StringUtils;

/**
 * An object of this class represents a graph instance within the Map-Reduce framework.
 * All settings about the graph, including the resident paths of the graph's data, are
 * wrapped with this class.
 */
public class Graph {
	static final public String simpleGraphKey        = "xrime.graph.simple";
	static final public String directedGraphKey      = "xrime.graph.directed";
	static final public String allowSelfloopKey      = "xrime.graph.allowSelfloop";
	static final public String allowMultiplyEdgesKey = "xrime.graph.allowMultiplyEdges";
	static final public String vertexClassKey        = "xrime.graph.vertex.class";
	static final public String egdeClassKey          = "xrime.graph.vertex.egde";
	static final public String graphPathsKey         = "xrime.graph.paths";
	
	/**
	 * Used to store settings of this graph.
	 */
	private Properties properties;
	
	/**
	 * Paths used to accommodate the persistent data of this graph. This info is also 
	 * encoded as a property of the graph.
	 */
	List<Path> paths;
	
	/**
	 * Default constructor.
	 */
	public Graph() {
		this.paths=new ArrayList<Path>();
		properties=new Properties();
	}
	
	/**
	 * Yet another constructor.
	 * @param properties properties of the graph.
	 */
	public Graph(Properties properties) {
		this.properties=(Properties) properties.clone();
		properties2GraphPaths();
	}
	
	/**
	 * Yet another constructor.
	 * @param graph source graph
	 */
	public Graph(Graph graph) {
		this.paths=new ArrayList<Path>(graph.getPaths());
		this.properties=(Properties) graph.getProperties().clone();
		properties2GraphPaths();
	}
	
	/**
	 * Retrieve all settings of this graph.	
	 * @return settings
	 */
	public Properties getProperties() {
		graphPaths2Properties();
		return properties;
	}
	
	/**
	 * Replace the settings of this graph with specified properties.
	 * @param properties graph settings.
	 */
	public void setProperties(Properties properties) {
		this.properties = properties;
		properties2GraphPaths();
	}
	
	/**
	 * Get a list of paths used to accommodate the persistent data of this graph.
	 * @return path list.
	 */
	public List<Path> getPaths() {
		return paths;
	}
	
	/**
	 * Get the only path used to accommodate the persistent data of this graph.
	 * @return  persistent data path.
	 * @throws IllegalAccessException If the graph data is stored in more than one directories, exception thrown.
	 */
	public Path getPath() throws IllegalAccessException {
		if(paths==null) {
			throw new IllegalAccessException("paths==null");
		}
		
		if(paths.size()!=1) {
			throw new IllegalAccessException("paths.size()!=1");
		}
		
		return paths.get(0);
	}
	
	/**
	 * Set the list of paths used to accommodate the persistent data of this graph.
	 * @param paths persistent data path list.
	 */
	public void setPaths(List<Path> paths) {
		this.paths = paths;
		graphPaths2Properties();
	}
	
	/**
	 * Set the only path used to accommodate the persistent data of this graph. 
	 *  @param path the path.
	 */
	public void setPath(Path path) {
		paths=new ArrayList<Path>();
		paths.add(path);
		graphPaths2Properties();
	}
	
	/**
	 * Add one more path to the list of paths of this graph.
	 * @param path the path.
	 */
	public void addPath(Path path) {
		if(paths==null) {
			paths=new ArrayList<Path>();
		}
		
		paths.add(path);
		graphPaths2Properties();
	}
	
	/**
	 * Check whether this graph is a simple graph.
	 * @return true for simple graph.
	 */
	public boolean isSimpleGraph() {
		return getBoolean(simpleGraphKey, true);
	}
	
	/**
	 * Set whether this graph is a simple graph. 
	 * X-Rime do not check the data. Please attention.
	 * @param simpleGraph simple graph flag. 
	 */
	public void setSimpleGraph(boolean simpleGraph) {
		setBoolean(simpleGraphKey, simpleGraph);
		if (simpleGraph) {
			setAllowSelfloop(false);
			setAllowMultipleEdges(false);
		}
	}
	
	/**
	 * Check whether this graph is a directed graph.
	 * @return true for directed graph.
	 */
	public boolean isDirectedGraph() {
		return getBoolean(directedGraphKey, true);
	}
	
	/**
	 * Set whether this graph is a directed graph.
	 * @param directedGraph directed graph flag. 
	 */
	public void setDirectedGraph(boolean directedGraph) {
		setBoolean(directedGraphKey, directedGraph);
	}
	
	/**
	 * Check whether self loops are allowed in this graph.
	 * @return true for allow selfloop graph.
	 */
	public boolean isAllowSelfloop() {
		return getBoolean(allowSelfloopKey, false);
	}
	
	/**
	 * Set whether self loops are allowed in this graph.
	 * @param allowSelfloop allow selfloop flag.
	 */
	public void setAllowSelfloop(boolean allowSelfloop) {
		setBoolean(allowSelfloopKey, allowSelfloop);
		if (allowSelfloop) {
			setSimpleGraph(false);
		}
	}
	
	/**
	 * Check whether multi-edges are allowed in this graph.
	 * @return true for allow multi-edges.
	 */
	public boolean isAllowMultipleEdges() {
		return getBoolean(allowMultiplyEdgesKey, false);
	}
	
	/**
	 * Set whether multi-edges are allowed in this graph.
	 * @param allowMultipleEdges allow multi-edges flag.
	 */
	public void setAllowMultipleEdges(boolean allowMultipleEdges) {
		setBoolean(allowMultiplyEdgesKey, allowMultipleEdges);
		if (allowMultipleEdges) {
			setSimpleGraph(false);
		}
	}
	
	private boolean getBoolean(String name, boolean defaultValue) {
	    String valueString = properties.getProperty(name);
	    if ("true".equals(valueString)) {
	        return true;
	    }
	    else if ("false".equals(valueString)) {
            return false;
	    }
	    
	    return defaultValue;
	}
	
	private void setBoolean(String name, boolean value) {
		properties.setProperty(name, Boolean.toString(value));
	}
	
	/**
	 * A default graph instance with default settings, except the paths.
	 */
	private static Graph defaultInstance;
	
	/**
	 * Get the default graph instance.
	 * Default graph in X-Rime is a directed simple graph.
	 * @return  default graph.
	 */
	public static Graph defaultGraph() {
		if(defaultInstance==null) {
			defaultInstance=new Graph();
			defaultInstance.setAllowMultipleEdges(false);
			defaultInstance.setAllowSelfloop(false);
			defaultInstance.setDirectedGraph(true);
		}
		
		return defaultInstance;
	}
	
	/**
	 * Parse the property value of the key "graphPathsKey", and added the resulting individual paths
	 * as elements of the "paths" array.
	 */
	private void properties2GraphPaths() {
		String pathsString=properties.getProperty(graphPathsKey);
		
		if(paths==null) {
			paths=new ArrayList<Path>();
		}
		
		if(pathsString!=null) {
			String[] strings=StringUtils.getStrings(pathsString);
			paths.clear();
			for(String path : strings) {
				paths.add(new Path(path));
			}
		}
	}
	
	/**
	 * Set the value of the property "graphPathsKey" with the content of the array
	 * "paths".
	 */
	private void graphPaths2Properties() {
		if(paths==null || paths.size()==0) {
			return;
		}
		
		String[] strings=new String[paths.size()];
		int ii=0;
		for(Path path: paths) {
			strings[ii++]=path.toString();
		}
		
		properties.setProperty(graphPathsKey, StringUtils.arrayToString(strings));
	}
}
