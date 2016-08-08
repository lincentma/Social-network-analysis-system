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
package org.sf.xrime.model.vertex;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableFactories;
import org.apache.hadoop.io.WritableFactory;
import org.sf.xrime.model.edge.AbstractEdge;
import org.sf.xrime.model.edge.Edge;


/**
 * Vertex with incidental edges. The edges could be incoming, outgoing, or both. 
 * All incidental edges are specified with both ends.
 */
public class AdjVertex extends Vertex implements Cloneable {
	/**
	 * Incidental edges of this vertex. All elements should have the same type (Edge or
	 * its derivants).
	 */
	protected List<Edge> edges;

	static {
		// Register writable factory of this class.
		WritableFactories.setFactory(AdjVertex.class, new WritableFactory() {
			public Writable newInstance() {
				return new AdjVertex();
			}
		});
	}
	
	/**
	 * Default constructor.
	 */
	public AdjVertex() {
		super();
		edges = new ArrayList<Edge>();
	}

	/**
	 * Normal constructor.
	 * @param id
	 */
	public AdjVertex(String id) {
		super(id);
		edges = new ArrayList<Edge>();		
	}
	
	/**
	 * Copy constructor.
	 * @param adjVertex
	 */
	public AdjVertex(AdjVertex adjVertex) {
		super(adjVertex.getId());
		edges = new ArrayList<Edge>();
		for (Edge edge : adjVertex.getEdges()) {
			edges.add((Edge) edge.clone());
		}
	}
	
	/**
	 * Get the list of incidental edges for this vertex.
	 * @return
	 */
	public List<Edge> getEdges() {
		return edges;
	}
	
	/**
	 * Replace the incidental edges of this vertex with specified list.
	 * @param edges
	 */
	public void setEdges(List<Edge> edges) {
		this.edges = edges;
	}
	
	/**
	 * Add an incidental edge to this vertex.
	 * @param edge
	 */
	public void addEdge(Edge edge) {
		if(edge.getFrom().compareTo(id)!=0 && edge.getTo().compareTo(id)!=0){
			// This edge has nothing to do with this vertex.
			return;
		}
		edges.add(edge);
	}
	
	/**
	 * Add incidental edges to this vertex.
	 * @param edges
	 */
	public void addEdges(List<Edge> edges) {
		for(Edge edge : edges){
			if(edge.getFrom().compareTo(id)!=0 && edge.getTo().compareTo(id)!=0){
				// This edge has nothing to do with this vertex.
			}else{
				this.edges.add(edge);
			}
		}
	}
	
	public void clearEdges() {
		edges.clear();
	}

	/**
	 * Remove loop (aka. self circle) on this vertex.
	 */
	public void removeLoop() {
		ArrayList<Edge> newEdges = new ArrayList<Edge>();
		for (Edge edge : edges) {
			if (edge.getTo().compareTo(id)!=0 || edge.getFrom().compareTo(id)!=0) {
				newEdges.add(edge);
			}
		}

		edges = newEdges;
	}
	
	/**
	 * Remove multi-edges incident to this vertex.
	 */
	public void removeMultipleEdge() {
		ArrayList<Edge> newEdges = new ArrayList<Edge>();
		Set<String> edgeStrs = new HashSet<String>();

		// Assume "->" is not allowed in any vertex id.
		for (Edge edge : edges) {
			String edgeStr = edge.getFrom()+"->"+edge.getTo();
			if (edgeStrs.contains(edgeStr)) {
				continue;
			}
			newEdges.add(edge);
			edgeStrs.add(edgeStr);
		}

		edges = newEdges;
	}

	public String toString() {
		String ret = "<" + id + ", <";
		for (Edge edge : edges) {
		  // Cast to Edge.
			ret += ((Edge)edge).toString();
			ret += ", ";
		}
		if (edges.size() > 0) {
			ret = ret.substring(0, ret.length() - 2) + ">>";
		} else {
			ret += ">>";
		}
		return ret;
	}
	  

	@Override
	public void fromString(String encoding){
	  // Clean.
	  id = null;
	  edges.clear();
	  
	  int pointerA = 0;
	  int pointerB = 0;
	  // Determine the id of vertex.
	  pointerB = encoding.indexOf(", ", pointerA);
	  id = encoding.substring(pointerA+1, pointerB);
	  pointerA = pointerB+3;
	  
	  // Edges part.
	  String edges_str = encoding.substring(pointerA, encoding.length()-2);
	  // A corner case.
	  if(edges_str.length()==0) return;
	  pointerA=0;
	  pointerB=0;
	  while(true){
	    pointerB = edges_str.indexOf(">, <", pointerA);
	    if(pointerB==-1){
	      // Reach the end of the string.
	      String edge_str = edges_str.substring(pointerA, edges_str.length());
	      int ft_delim_index = edge_str.indexOf(", ");
	      String from = edge_str.substring(1, ft_delim_index);
	      String to = edge_str.substring(ft_delim_index+2, edge_str.length()-1);
	      edges.add(new Edge(from, to));
	      return;
	    }else{
	      // In the middle of the string.
	      String edge_str = edges_str.substring(pointerA, pointerB+1);
	      int ft_delim_index = edge_str.indexOf(", ");
	      String from = edge_str.substring(1, ft_delim_index);
	      String to = edge_str.substring(ft_delim_index+2, edge_str.length()-1);
	      edges.add(new Edge(from, to));
	    }
	    // Move forward the pointer.
	    pointerA = pointerB+3;
	  }	}

	public Object clone() {
		return new AdjVertex(this);
	}

	@SuppressWarnings("unchecked")
	public void readFields(DataInput in) throws IOException {
		super.readFields(in);
		// Clear the container.
		edges.clear();
		// Determine the size.
		int size = in.readInt();

		if (size > 0) {
			// Determine the element type.
			String className = Text.readString(in);
			try {
				for (int ii = 0; ii < size; ii++) {
					Class instanceClass = Class.forName(className);
					Writable writable = WritableFactories.newInstance(
							instanceClass, null);
					writable.readFields(in);
					if (writable instanceof Edge) {
						addEdge((Edge) writable);
					}
				}
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			}
		}
	}

	public void write(DataOutput out) throws IOException {
		// super.
		super.write(out);

		if (edges == null) {
			out.writeInt(0);
			return;
		}
		// number of incidental edges.
		out.writeInt(edges.size());
		if (edges.size() > 0) {
			// All incidental edges should have the same type.
			Text.writeString(out, edges.get(0).getClass().getName());
			for (Edge edge : edges) {
				edge.write(out);
			}
		}
	}

	@Override
	public Iterator<AbstractEdge>  getIncidentElements() {
		/**
		 * Internal class used to implement customized iterator logic.
		 */
		class Itr implements Iterator<AbstractEdge> {
			Iterator<Edge> itr;

			public Itr() {
				itr=null;
			}

			@Override
			public boolean hasNext() {
				if (itr==null) {
					return false;
				}

				return itr.hasNext();
			}

			@Override
			public Edge next() {
				if (itr==null) {
					return null;
				}

				return itr.next();
			}

			@Override
			public void remove() {	
				throw new UnsupportedOperationException("This is a read-only iterator");
			}	

			public void setItr(Iterator<Edge> itr) {
				this.itr = itr;
			}
		}

		Itr ret=new Itr();
		if(edges!=null) {
			ret.setItr(edges.iterator());
		}

		return ret;		
	}
}
