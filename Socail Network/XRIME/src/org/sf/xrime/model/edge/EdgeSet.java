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
package org.sf.xrime.model.edge;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableFactories;
import org.apache.hadoop.io.WritableFactory;
import org.sf.xrime.model.Element;


/**
 * A set of edges.
 * @author xue
 */
public class EdgeSet implements Element, Cloneable {
  /**
   * Internal set. All its elements should have the same type.
   */
	private Set<Edge> _edges = null;

	static {
	  // Register the writable factory for this class.
		WritableFactories.setFactory(EdgeSet.class, new WritableFactory() {
			public Writable newInstance() {
				return new EdgeSet();
			}
		});
	}

	/**
	 * Basic constructor.
	 */
	public EdgeSet() {
		_edges = new HashSet<Edge>();
	}

	/**
	 * Copy constructor.
	 * 
	 * @param set
	 */
	public EdgeSet(EdgeSet set) {
		_edges = new HashSet<Edge>();
		for (Edge edge : set._edges) {
			_edges.add((Edge) edge.clone());
		}
	}
	/**
	 * Get the set of edges.
	 * @return
	 */
	public Set<Edge> getEdges() {
		return _edges;
	}
	/**
	 * Replace internal set with specified set.
	 * @param set
	 */
	public void setEdges(Set<Edge> set) {
		_edges = set;
	}
	/**
	 * Add an edge to this set.
	 * @param edge 
	 */
	public void addEdge(Edge edge) {
		_edges.add(edge);
	}
	/**
	 * Add all edges in specified set to this set.
	 * @param set
	 */
	public void addEdges(Set<Edge> set) {
		_edges.addAll(set);
	}
	/**
	 * Clear this edge set.
	 */
	public void clear() {
		_edges.clear();
	}

	@Override
	protected Object clone() throws CloneNotSupportedException {
		return new EdgeSet(this);
	}

	/**
	 * Despite the class of edges, treate them as Edge.
	 */	@Override
	public String toString() {
		StringBuffer result_buf = new StringBuffer();
		result_buf.append("(");
		for (Edge edge : _edges) {
		  // Cast to Edge.
			result_buf.append(((Edge)edge).toString());
			result_buf.append(", ");
		}
		if (_edges.size() > 0)
			result_buf.delete(result_buf.length() - 2, result_buf.length());
		result_buf.append(")");
		return result_buf.toString();
	}
	
	/**
	 * Despite the class of edges, treate them as Edge.
	 */
  @Override
  public void fromString(String encoding) {
    // Clean.
    _edges.clear();
    
    String edges_str = encoding.substring(1, encoding.length()-1);
    // Empty edge set.
    if(edges_str.length()==0) return;
    
    int pointerA = 0, pointerB = 0;
    while(true){
      pointerB = edges_str.indexOf(">, <", pointerA);
      Edge new_edge = new Edge();
      if(pointerB==-1){
        // The end of edges.
        new_edge.fromString(edges_str.substring(pointerA, edges_str.length()));
        _edges.add(new_edge);
        return;
      }else{
        new_edge.fromString(edges_str.substring(pointerA, pointerB+1));
        _edges.add(new_edge);
      }
      pointerA = pointerB + 3;
    }
  }

	@SuppressWarnings("unchecked")
	@Override
	public void readFields(DataInput in) throws IOException {
	  // Clear the container.
		_edges.clear();
		// Determine the size.
		int size = in.readInt();
		if (size > 0) {
		  // All edges in the set should have the same type.
			String className = Text.readString(in);
			try {
				Class instanceClass;
				instanceClass = Class.forName(className);
				for (int i = 0; i < size; i++) {
					Writable writable = WritableFactories.newInstance(
							instanceClass, null);
					writable.readFields(in);
					if (writable instanceof Edge) {
						addEdge((Edge)writable);
					}
				}
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			}
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {
		if (_edges == null || _edges.size() == 0) {
			out.writeInt(0);
		} else {
		  // Write the number of edges in this set.
			out.writeInt(_edges.size());
			// All the edges should have the same type.
			Text.writeString(out, _edges.toArray()[0].getClass().getName());
			for (Edge edge : _edges) {
				edge.write(out);
			}
		}
	}

	/* 
	 * Since we do not really own any incident element, so hasNext() is always return false.
	 * @see org.sf.xrime.model.Element#getIncidentElements()
	 */
	@Override
	public Iterator<Element> getIncidentElements() {		
	  /**
	   * An internal class used to implement customized iterator logic.
	   */
		class Itr implements Iterator<Element> {
			@Override
			public boolean hasNext() {
				// we own nothing.
				return false;
			}

			@Override
			public Element next() {
				return null;
			}

			@Override
			public void remove() {		
				throw new UnsupportedOperationException("This is a read-only iterator");
			}			
		}
		
		return new Itr();
	}

}
