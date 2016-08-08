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
import java.util.Iterator;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableFactories;
import org.apache.hadoop.io.WritableFactory;
import org.sf.xrime.model.edge.AbstractEdge;
import org.sf.xrime.model.edge.AdjVertexEdge;
import org.sf.xrime.model.edge.AdjVertexEdgeComparator;
import org.sf.xrime.model.edge.Edge;


/**
 * Vertex with incidental edges. The edges could be incoming, outgoing or both.
 * Edges are represented with AdjVertexEdge, which only record the other end of
 * the edge in contrast with this vertex.
 */
public class AdjSetVertex extends Vertex implements Cloneable {
  /**
   * The other ends, aka opposites, of incidental edges. All elements of this set
   * should have the same type (AdjVertexEdge or its derivants).
   */
	protected Set<AdjVertexEdge> opposites = null;
	
	static {
	  // Register writable factory for this class.
		WritableFactories.setFactory(AdjSetVertex.class, new WritableFactory() {
			public Writable newInstance() {
				return new AdjSetVertex();
			}
		});
	}
	/**
	 * Default constructor.
	 */
	public AdjSetVertex() {
	  super();
		opposites = new TreeSet<AdjVertexEdge>(new AdjVertexEdgeComparator());
	}
	/**
	 * Normal constructor.
	 * @param id
	 */
	public AdjSetVertex(String id){
	  super(id);
	  opposites = new TreeSet<AdjVertexEdge>(new AdjVertexEdgeComparator());
	}
	/**
	 * Copy constructor.
	 * @param vertex
	 */
	public AdjSetVertex(AdjSetVertex vertex) {
		super(vertex);
	  opposites = new TreeSet<AdjVertexEdge>(new AdjVertexEdgeComparator());
	  for(AdjVertexEdge edge : vertex.getOpposites()){
	    opposites.add((AdjVertexEdge) edge.clone());
	  }
	}
	
	/**
	 * Initialize this object with the content of specified AdjVertex. Particularly,
	 * only to ends of outgoing incidental edges are reserved. This method could be
	 * used when the input AdjVertex only records outgoing incidental edges.
	 * @param vertex
	 */
	public void fromAdjVertexTos(AdjVertex vertex) {
		id = vertex.getId();
		opposites.clear();

		for (Edge edge : vertex.getEdges()) {
			opposites.add(new AdjVertexEdge(edge.getTo()));
		}
	}
	
	/**
	 * Initialize this object with the content of specified AdjVertex. Particularly,
	 * only from ends of incoming incidental edges are reserved. This method could 
	 * be used when the input AdjVertex only records incoming incidental edges.
	 * @param vertex
	 */	public void fromAdjVertexFroms(AdjVertex vertex){
	  id = vertex.getId();
	  opposites.clear();
	  for(Edge edge : vertex.getEdges()){
	    opposites.add(new AdjVertexEdge(edge.getFrom()));
	  }
	}
	/**
	 * Initialize this object with the content of specified AdjVertex. Particularly,
	 * both from ends of incoming incidental edges, and to ends of outgoing incidental
	 * edges are reserved. It should be noted that, this vertex itself should only be
	 * reserved when there exists a loop.
	 * @param vertex
	 */
	public void fromAdjVertexBoth(AdjVertex vertex){
	  id = vertex.getId();
	  opposites.clear();
	  for(Edge edge : vertex.getEdges()){
	    if(edge.getFrom().compareTo(id)!=0){
	      opposites.add(new AdjVertexEdge(edge.getFrom()));
	    }else if(edge.getTo().compareTo(id)!=0){
	      opposites.add(new AdjVertexEdge(edge.getTo()));
	    }else{
	      // A loop.
	      opposites.add(new AdjVertexEdge(id));
	    }
	  }
	}
	/**
	 * Get opposite vertexes of incidental edges of this vertex.
	 * @return
	 */
	public Set<AdjVertexEdge> getOpposites() {
		return opposites;
	}
	/**
	 * Replace the set of opposite vertexes with specified one.
	 * @param tos
	 */
	public void setOpposites(Set<AdjVertexEdge> tos) {
		this.opposites = tos;
	}
	/**
	 * Add an opposite (the other end of an incidental edge) to this vertex.
	 * @param to
	 */
	public void addOpposite(AdjVertexEdge to) {
		opposites.add(to);
	}
	
	/**
	 * Clear all opposites.
	 */
	public void clearOpposites(){
	  opposites.clear();
	}
	
	/**
	 * Remove loop on this vertex.
	 */
	public void removeLoop() {
		opposites.remove(id);
	}

	public String toString() {
		String ret = "<" + id + ", <";
		for (AdjVertexEdge sibling : opposites) {
		  // Cast to AdjVertexEdge.
			ret += ((AdjVertexEdge)sibling).toString();
			ret += ", ";
		}

		if (opposites.size() > 0) {
			ret = ret.substring(0, ret.length() - 2) + ">>";
		} else {
			ret += ">>";
		}

		return ret;
	}
	
	@Override
	public void fromString(String encoding){
	  // Clean.
	  this.id = null;
	  this.opposites.clear();
	  
	  // Get vertex id.
	  int pointerA = 0, pointerB = 0;
	  pointerB = encoding.indexOf(", <", pointerA);
	  this.id = encoding.substring(pointerA+1, pointerB);
	  
	  // Get opposite vertexes.
	  String opps_str = encoding.substring(pointerB+3, encoding.length()-2);
	  pointerA=0;
	  pointerB=0;
	  // None neighbors.
	  if(opps_str.length()==0) return;
	  while(true){
	    pointerB = opps_str.indexOf(", ", pointerA);
	    if(pointerB == -1){
	      // The end of oppsite vertexes.
	      this.opposites.add(new AdjVertexEdge(opps_str.substring(pointerA, opps_str.length())));
	      return;
	    }else{
	      // In the middle of opposite vertexes list.
	      this.opposites.add(new AdjVertexEdge(opps_str.substring(pointerA, pointerB)));
	    }
	    pointerA = pointerB+2;
	  }
	}

	public Object clone() {
		return new AdjSetVertex(this);
	}

	@SuppressWarnings("unchecked")
  public void readFields(DataInput in) throws IOException {
	  // super.
		super.readFields(in);
		// Clear the container.
		opposites.clear();
		// Determine container size.
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
					if (writable instanceof AdjVertexEdge) {
						addOpposite((AdjVertexEdge) writable);
					}
				}
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			}
		}	}

	public void write(DataOutput out) throws IOException {
	  // super.
		super.write(out);

		if (opposites == null) {
			out.writeInt(0);
			return;
		}
		// Size of the container.
		out.writeInt(opposites.size());
		if (opposites.size() > 0) {
		  // All incidental edges should have the same type.
			Text.writeString(out, opposites.toArray()[0].getClass().getName());
			for (AdjVertexEdge sibling : opposites) {
			  sibling.write(out);
			}
		}
	}
	
	@Override
	public Iterator<AbstractEdge>  getIncidentElements() {
		class Itr implements Iterator<AbstractEdge> {
			Iterator<AdjVertexEdge> itr;
			public Itr(){
			  itr = null;
			}
						
			@Override
			public boolean hasNext() {
				if(itr==null) {
				    return false;
				}
				
				return itr.hasNext();
			}

			@Override
			public AbstractEdge next() {
				if(itr==null) {
				    return null;
				}
				
				return itr.next();
			}

			@Override
			public void remove() {
				throw new UnsupportedOperationException("This is a read-only iterator");
			}

			public void setItr(Iterator<AdjVertexEdge> itr) {
				this.itr = itr;
			}	
		}
		
		Itr ret=new Itr();
		
		if(opposites!=null) {
			ret.setItr(opposites.iterator());
		}

		return ret;
	}
}
