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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableFactories;
import org.apache.hadoop.io.WritableFactory;
import org.sf.xrime.model.Element;
import org.sf.xrime.model.vertex.Vertex;

/**
 * Edge used in AdjSetVertexWithLabel, which only record the opposite vertex's id and 
 * corresponding label. The opposite vertex could be the from end, or the to end of a 
 * directed edge.
 * 
 * The initial purpose of this data structure is for implementing KDD09's paper "Parallel 
 * Community Detection on Large Networks with Propinquity Dynamics" 
 * 
 * 
 * @author Cai Bin
 * @author juwei
 */
public class AdjVertexEdgeWithLabel extends AbstractEdge implements Cloneable {
	/**
	 * The id of the opposite vertex of this edge, which could be the from end,
	 * or the to end of the edge.
	 */
	protected String opposite;
	
	/**
	 * The label of the opposite vertex, which could be arbitrary value. For example, the 
	 * propinquity between the source vertex and the opposite vertex. 
	 */
	protected int label = 0;
	
	private static String nullString = "";

	static {
		// Register writable factory for this class.
		WritableFactories.setFactory(AdjVertexEdgeWithLabel.class,
				new WritableFactory() {
					public Writable newInstance() {
						return new AdjVertexEdgeWithLabel();
					}
				});
	}

	/**
	 * Trivial constructor.
	 */
	public AdjVertexEdgeWithLabel() {
	}
	
	/**
	 * Constructor with opposite vertex's id only.
	 * 
	 * @param opposite
	 *            opposite vertex's id.
	 */
	public AdjVertexEdgeWithLabel(String opposite) {
		this.opposite = opposite;
	}

	/**
	 * Constructor with opposite vertex's id and label.
	 * 
	 * @param opposite
	 *            opposite vertex's id.
	 * @param label
	 */
	public AdjVertexEdgeWithLabel(String opposite, int label) {
		this.opposite = opposite;
		this.label = label;
	}

	/**
	 * Copy constructor.
	 * 
	 * @param AdjVertexEdgeWithLabel
	 */
	public AdjVertexEdgeWithLabel(AdjVertexEdgeWithLabel adjVertexEdgeWithLabel) {
		this.opposite = adjVertexEdgeWithLabel.opposite;
		this.label = adjVertexEdgeWithLabel.label;
	}

	/**
	 * Get the id of the vertex on the other end of this edge.
	 * 
	 * @return
	 */
	public String getOpposite() {
		return opposite;
	}

	/**
	 * Set the id of the vertex on the other end.
	 * 
	 * @param opposite
	 */
	public void setOpposite(String opposite) {
		this.opposite = opposite;
	}
	
	/**
	 * Get the label of the vertex on the other end of this edge.
	 * 
	 * @return
	 */
	public int getLabel() {
		return label;
	}
	
	/**
	 * Set the id of the vertex on the other end.
	 * 
	 * @param label
	 */
	public void setLabel(int label) {
		this.label = label;
	}

	@Override
	public String toString(){
		StringBuffer buffer = new StringBuffer();
		buffer.append("<");
		if (opposite != null){
			buffer.append(opposite);
		}
		buffer.append(", ");
		buffer.append(label+"");
		buffer.append(">");
		return buffer.toString();
	}

	@Override
	public void fromString(String encoding) {
		// Clean
		this.opposite = null;
		this.label = 0;		

		if (encoding.length() <= 3)
			return;

		int comma_index = encoding.indexOf(", ");
		this.opposite = encoding.substring(1, comma_index);
		this.label = Integer.parseInt(encoding.substring(comma_index+2, encoding.length()-1));
	}

	public Object clone() {
		return new AdjVertexEdgeWithLabel(this);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		opposite = Text.readString(in);
		if (opposite.length() == 0){
			opposite = null;
		}
		label = in.readInt();
		//this.label = Text.readString(in);
		//if (label.length() == 0){
		//	label = null;
		//}
	}

	@Override
	public void write(DataOutput out) throws IOException {
		if (opposite == null){
			Text.writeString(out, nullString);
		}else{
			Text.writeString(out, opposite);
		}
		out.writeInt(label);
		
		//if (label == null){
		//	Text.writeString(out, nullString);
		//}else{
		//	Text.writeString(out, label);
		//}
	}

	@Override
	public Iterator<? extends Element> getIncidentElements() {
		/**
		 * Internal class used to implement customized iterator logic.
		 */
		class Itr implements Iterator<Vertex> {
			Iterator<Vertex> itr;

			public Itr(Iterator<Vertex> itr) {
				this.itr = itr;
			}

			@Override
			public boolean hasNext() {
				return itr.hasNext();
			}

			@Override
			public Vertex next() {
				return itr.next();
			}

			@Override
			public void remove() {
				throw new UnsupportedOperationException(
						"This is a read-only iterator");
			}
		}
		// Only iterator on the id of the opposite vertex.
		List<Vertex> vertexs = new ArrayList<Vertex>();
		vertexs.add(new Vertex(opposite));

		Itr itr = new Itr(vertexs.iterator());

		return itr;
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof AdjVertexEdgeWithLabel) {
			return opposite.equals(((AdjVertexEdgeWithLabel) obj).getOpposite())
					&& (label == (((AdjVertexEdgeWithLabel) obj).getLabel()));
		} else {
			return false;
		}
	}

	@Override
	public int hashCode() {
		return (opposite+label).hashCode();
	}

}
