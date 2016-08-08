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
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableFactories;
import org.apache.hadoop.io.WritableFactory;
import org.sf.xrime.model.edge.AbstractEdge;
import org.sf.xrime.model.edge.AdjVertexEdgeWithLabel;
import org.sf.xrime.model.edge.AdjVertexEdgeWithLabelComparator;
import org.sf.xrime.model.edge.Edge;

/**
 * 1. record the vertex id and corresponding label of its neighbors. 2. record
 * the vertex id and corresponding label of its neighbors' neighbors. (not
 * exactly two-hop neighbors?)
 * 
 * The initial purpose of this data structure is for implementing KDD09's paper
 * "Parallel Community Detection on Large Networks with Propinquity Dynamics"
 * 
 * @author juwei
 */
public class AdjSetVertexWithTwoHopLabel extends Vertex implements Cloneable {

	/**
	 * The other ends, aka opposites, of incidental edges. All elements of this
	 * set should have the same type (AdjVertexEdgeWithLabel or its derivants).
	 */
	protected Set<AdjVertexEdgeWithLabel> neighbors = null;

	/**
	 * The neigbors's neighbors, we use "twoHopNeighbors" to denote it.
	 */
	protected Map<String, Set<AdjVertexEdgeWithLabel>> allTwoHopNeighbors = null;

	static {
		// Register writable factory for this class.
		WritableFactories.setFactory(AdjSetVertexWithTwoHopLabel.class,
				new WritableFactory() {
					public Writable newInstance() {
						return new AdjSetVertexWithTwoHopLabel();
					}
				});
	}

	/**
	 * Default constructor.
	 */
	public AdjSetVertexWithTwoHopLabel() {
		super();
		neighbors = new TreeSet<AdjVertexEdgeWithLabel>(
				new AdjVertexEdgeWithLabelComparator());
		allTwoHopNeighbors = new HashMap<String, Set<AdjVertexEdgeWithLabel>>();
	}

	/**
	 * Normal constructor.
	 * 
	 * @param id
	 */
	public AdjSetVertexWithTwoHopLabel(String id) {
		super(id);
		neighbors = new TreeSet<AdjVertexEdgeWithLabel>(
				new AdjVertexEdgeWithLabelComparator());
		allTwoHopNeighbors = new HashMap<String, Set<AdjVertexEdgeWithLabel>>();
	}

	/**
	 * Copy constructor. (deep copy)
	 * 
	 * @param vertex
	 */
	public AdjSetVertexWithTwoHopLabel(AdjSetVertexWithTwoHopLabel vertex) {
		// copy vertex id
		super(vertex.getId());
		// copy neighbors
		neighbors = new TreeSet<AdjVertexEdgeWithLabel>(
				new AdjVertexEdgeWithLabelComparator());
		for (AdjVertexEdgeWithLabel edge : vertex.getNeighbors()) {
			neighbors.add((AdjVertexEdgeWithLabel) edge.clone());
		}
		// copy neighbors' neighbors
		allTwoHopNeighbors = new HashMap<String, Set<AdjVertexEdgeWithLabel>>();
		for (String key : vertex.getAllTwoHopNeighbors().keySet()) {
			Set<AdjVertexEdgeWithLabel> twoHopNeighbors = vertex
					.getSepecificTwoHopNeighbors(key);
			Set<AdjVertexEdgeWithLabel> newTwoHopNeighbors = new TreeSet<AdjVertexEdgeWithLabel>(
					new AdjVertexEdgeWithLabelComparator());
			for (AdjVertexEdgeWithLabel edge : twoHopNeighbors) {
				newTwoHopNeighbors.add((AdjVertexEdgeWithLabel) edge.clone());
			}
			allTwoHopNeighbors.put(key, newTwoHopNeighbors);
		}
	}

	/**
	 * Initialize this object with the content of specified AdjVertex.
	 * Particularly, only to ends of outgoing incidental edges are reserved.
	 * This method could be used when the input AdjVertex only records outgoing
	 * incidental edges.
	 * 
	 * Note that we need to perform pre-processing by MapReduce to construct the
	 * 2-hop structures. Initially there is only 1-hop record (i.e. neighbors)
	 * in this object.
	 * 
	 * @param vertex
	 */
	public void fromAdjVertexTos(AdjVertex vertex) {
		id = vertex.getId();
		neighbors.clear();
		for (Edge edge : vertex.getEdges()) {
			neighbors.add(new AdjVertexEdgeWithLabel(edge.getTo()));
		}
	}

	/**
	 * Initialize this object with the content of specified AdjVertex.
	 * Particularly, only from ends of incoming incidental edges are reserved.
	 * This method could be used when the input AdjVertex only records incoming
	 * incidental edges.
	 * 
	 * Note that we need to perform pre-processing by MapReduce to construct the
	 * 2-hop structures. Initially there is only 1-hop record (i.e. neighbors)
	 * in this object.
	 * 
	 * @param vertex
	 */
	public void fromAdjVertexFroms(AdjVertex vertex) {
		id = vertex.getId();
		neighbors.clear();
		for (Edge edge : vertex.getEdges()) {
			neighbors.add(new AdjVertexEdgeWithLabel(edge.getFrom()));
		}
	}

	/**
	 * Initialize this object with the content of specified AdjVertex.
	 * Particularly, both from ends of incoming incidental edges, and to ends of
	 * outgoing incidental edges are reserved. It should be noted that, this
	 * vertex itself should only be reserved when there exists a loop.
	 * 
	 * Note that we need to perform pre-processing by MapReduce to construct the
	 * 2-hop structures. Initially there is only 1-hop record (i.e. neighbors)
	 * in this object.
	 * 
	 * @param vertex
	 */
	public void fromAdjVertexBoth(AdjVertex vertex) {
		id = vertex.getId();
		neighbors.clear();
		for (Edge edge : vertex.getEdges()) {
			if (edge.getFrom().compareTo(id) != 0) {
				neighbors.add(new AdjVertexEdgeWithLabel(edge.getFrom()));
			} else if (edge.getTo().compareTo(id) != 0) {
				neighbors.add(new AdjVertexEdgeWithLabel(edge.getTo()));
			} else {
				// A loop.
				neighbors.add(new AdjVertexEdgeWithLabel(id));
			}
		}
	}

	/**
	 * Get opposite vertexes of incidental edges of this vertex.
	 * 
	 * @return
	 */
	public Set<AdjVertexEdgeWithLabel> getNeighbors() {
		return neighbors;
	}

	/**
	 * Replace the set of neighbors with specified one.
	 * 
	 * @param tos
	 */
	public void setNeighbors(Set<AdjVertexEdgeWithLabel> tos) {
		neighbors = tos;
	}

	/**
	 * Add a neighbor (the other end of an incidental edge) to this vertex.
	 * 
	 * @param to
	 */
	public void addNeighbor(AdjVertexEdgeWithLabel to) {
		neighbors.add(to);
	}

	/**
	 * get all two-hop neighbors
	 * 
	 * @return HashMap of neigbhors
	 */
	public Map<String, Set<AdjVertexEdgeWithLabel>> getAllTwoHopNeighbors() {
		return allTwoHopNeighbors;
	}

	/**
	 * get two-hop neighbors of a specific neighbor
	 * 
	 * @return Set<AdjVertexEdgeWithLabel>
	 */
	public Set<AdjVertexEdgeWithLabel> getSepecificTwoHopNeighbors(String id) {
		return allTwoHopNeighbors.get(id);
	}

	/**
	 * Replce the set of allTwoHopNeighbors with specified one
	 * 
	 * @param allTwoHopNeighbors
	 */
	public void setAllTwoHopNeighbors(
			Map<String, Set<AdjVertexEdgeWithLabel>> all) {
		allTwoHopNeighbors = all;
	}

	/**
	 * Add a set of two hop neighbors of a vertex
	 * 
	 * @para neighbors
	 */
	public void addSepecificTwoHopNeighbors(String id,
			Set<AdjVertexEdgeWithLabel> neighbors) {
		allTwoHopNeighbors.put(id, neighbors);
	}

	/**
	 * remove a set of two hop neighbors of a vertex
	 * 
	 * @para neighbors
	 */
	public void removeSepecificTwoHopNeighbors(String id) {
		allTwoHopNeighbors.remove(id);
	}

	/**
	 * Clear all neighbors.
	 */
	public void clearNeighbors() {
		neighbors.clear();
	}

	/**
	 * Clear all two hop neighbors
	 */
	public void clearAllTwoHopNeighbors() {
		allTwoHopNeighbors.clear();
	}

	/**
	 * Remove loop on this vertex.
	 */
	public void removeLoop() {
		neighbors.remove(id);
		allTwoHopNeighbors.remove(id);
	}

	/**
	 * support 2-hop print
	 */
	public String toString() {
		String ret = "<" + id + "; neighbors: {";
		for (AdjVertexEdgeWithLabel sibling : neighbors) {
			// Cast to AdjVertexEdge.
			ret += ((AdjVertexEdgeWithLabel) sibling).toString();
			ret += ", ";
		}
		if (neighbors.size() > 0) {
			ret = ret.substring(0, ret.length() - 2) + "}";
		} else {
			ret += "}";
		}
		ret+= "; 2-hop neighbors: {";
		for (String nb: allTwoHopNeighbors.keySet()){
			ret += "(" +nb + " - ";
			for (AdjVertexEdgeWithLabel sb: allTwoHopNeighbors.get(nb)){
				ret += ((AdjVertexEdgeWithLabel) sb).toString();
				ret += ", ";
			}
			ret += ")";
		}
		if (allTwoHopNeighbors.size() > 0) {
			ret = ret.substring(0, ret.length() - 3) + ")}>";
		} else{
			ret += "}>";
		}
		return ret;
	}

	/**
	 * only support 1-hop encoding
	 */
	@Override
	@Deprecated
	public void fromString(String encoding) {
		// Clean.
		this.id = null;
		this.neighbors.clear();
		this.allTwoHopNeighbors.clear();

		// Get vertex id.
		int pointerA = 0, pointerB = 0;
		pointerB = encoding.indexOf(", <", pointerA);
		this.id = encoding.substring(pointerA + 1, pointerB);

		// Get opposite vertexes.
		String opps_str = encoding.substring(pointerB + 3,
				encoding.length() - 2);
		pointerA = 0;
		pointerB = 0;
		// None neighbors.
		if (opps_str.length() == 0)
			return;
		while (true) {
			pointerB = opps_str.indexOf(", ", pointerA);
			if (pointerB == -1) {
				// The end of oppsite vertexes.
				this.neighbors.add(new AdjVertexEdgeWithLabel(opps_str
						.substring(pointerA, opps_str.length())));
				return;
			} else {
				// In the middle of opposite vertexes list.
				this.neighbors.add(new AdjVertexEdgeWithLabel(opps_str
						.substring(pointerA, pointerB)));
			}
			pointerA = pointerB + 2;
		}
	}

	public Object clone() {
		return new AdjSetVertexWithTwoHopLabel(this);
	}

	@SuppressWarnings("unchecked")
	public void readFields(DataInput in) throws IOException {
		// super.
		super.readFields(in);
		//System.out.println("Invoking readFields() for De-serialization, node = "+this.id);
		// Clear the container.
		neighbors.clear();
		allTwoHopNeighbors.clear();
		if (this.id.equals("")){
			this.id = null;
		}
		else{
			// Determine container size.
			int size = in.readInt();
			//System.out.println("neighbors.size() = "+size);
			if (size > 0) {
				//Determine the element type, read neighbors
				String className = Text.readString(in);
				//System.out.println("ClassName = "+className);
				try {
					for (int ii = 0; ii < size; ii++) {
						Class instanceClass = Class.forName(className);
						Writable writable = WritableFactories.newInstance(
								instanceClass, null);
						writable.readFields(in);
						if (writable instanceof AdjVertexEdgeWithLabel) {
							this.addNeighbor((AdjVertexEdgeWithLabel) writable);
							//System.out.println("addNeighbor(): "+((AdjVertexEdgeWithLabel) writable).getOpposite());
						}
					}
				} catch (ClassNotFoundException e) {
					e.printStackTrace();
				}

				// read two hop neighbors
				int mapSize = in.readInt();
				//System.out.println("twoHopNeihgbor map size = " + mapSize);
				while (mapSize > 0) {
					String currentKey = Text.readString(in);
					int neighborSize = in.readInt();
					if (neighborSize > 0) {
						String currentClassName = Text.readString(in);
						Set<AdjVertexEdgeWithLabel> currentTwoHopNeighbors = new TreeSet<AdjVertexEdgeWithLabel>(
								new AdjVertexEdgeWithLabelComparator());
						try {
							for (int jj = 0; jj < neighborSize; jj++) {
								Class currentInstanceClass = Class
										.forName(currentClassName);
								Writable currentWritable = WritableFactories
										.newInstance(currentInstanceClass, null);
								currentWritable.readFields(in);
								if (currentWritable instanceof AdjVertexEdgeWithLabel) {
									currentTwoHopNeighbors
											.add((AdjVertexEdgeWithLabel) currentWritable);
								}
							}
						} catch (ClassNotFoundException e) {
							e.printStackTrace();
						}
						allTwoHopNeighbors.put(currentKey, currentTwoHopNeighbors);
						mapSize--;
					}
				}
			}
		}
	}

	public void write(DataOutput out) throws IOException {
		//System.out.println("Invoking write() for Serialization, node = "+this.id);
		// super.
		if (this.id==null){
			Text.writeString(out, "");
		}else{
			super.write(out);
			// serialize neighbors
			if (neighbors == null){
				out.writeInt(0);
				return;
			}
			//System.out.println("neighbors.size() = "+neighbors.size());
			out.writeInt(neighbors.size());// Size of the container.
			if (neighbors.size() > 0){
				// All incidental edges should have the same type.
				Text.writeString(out, neighbors.toArray()[0].getClass().getName());
				for (AdjVertexEdgeWithLabel sibling : neighbors) {
					//System.out.println("wirte neighbor: " + sibling.getOpposite());
					sibling.write(out);
				}
			}
			// serialize allTwoHopNeighbors
			//format:
			// |----------------------------------------------------------------|
			// | map size,														|
			// | key1, neighbor size, neighbor class, neigbor 1, neighbor 2, ...|
			// | key2, neighbor size, neighbor class, neigbor 3, neighbor 4, ...|
			// |----------------------------------------------------------------|
			if (allTwoHopNeighbors == null) {
				//System.out.println("allTwoHopNeighbors=0, out.write(0)");
				out.writeInt(0);
				return;
			}
			//System.out.println("allTwoHopNeighbors.size()="+allTwoHopNeighbors.size());
			out.writeInt(allTwoHopNeighbors.size());
			if (allTwoHopNeighbors.size() > 0){
				for (String key : allTwoHopNeighbors.keySet()) {
					Text.writeString(out, key);
					Set<AdjVertexEdgeWithLabel> twoHopNeighbors = allTwoHopNeighbors
							.get(key);
					if (twoHopNeighbors == null) {
						out.writeInt(0);
						return;
					}
					out.writeInt(twoHopNeighbors.size());
					if (twoHopNeighbors.size() > 0) {
						Text.writeString(out, twoHopNeighbors.toArray()[0]
								.getClass().getName());
						for (AdjVertexEdgeWithLabel sibling : twoHopNeighbors) {
							sibling.write(out);
						}
					}
				}
			}
		}
		
	}

	@Override
	public Iterator<AbstractEdge> getIncidentElements() {
		class Itr implements Iterator<AbstractEdge> {
			Iterator<AdjVertexEdgeWithLabel> itr;

			public Itr() {
				itr = null;
			}

			@Override
			public boolean hasNext() {
				if (itr == null) {
					return false;
				}
				return itr.hasNext();
			}

			@Override
			public AbstractEdge next() {
				if (itr == null) {
					return null;
				}

				return itr.next();
			}

			@Override
			public void remove() {
				throw new UnsupportedOperationException(
						"This is a read-only iterator");
			}

			public void setItr(Iterator<AdjVertexEdgeWithLabel> itr) {
				this.itr = itr;
			}
		}

		Itr ret = new Itr();

		if (neighbors != null) {
			ret.setItr(neighbors.iterator());
		}

		return ret;
	}
}
