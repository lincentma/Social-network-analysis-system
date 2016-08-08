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
 * Edge used in AdjSetVertex, which only record the opposite vertex's id. The
 * opposite vertex could be the from end, or the to end of a directed edge.
 * @author Cai Bin
 */
public class AdjVertexEdge extends AbstractEdge implements Cloneable {
  /**
   * The id of the opposite vertex of this edge, which could be the from end,
   * or the to end of the edge.
   */
	protected String opposite;
	
	static {
	  // Register writable factory for this class.
		WritableFactories.setFactory
		      (AdjVertexEdge.class,
		       new WritableFactory() {
		           public Writable newInstance() { return new AdjVertexEdge(); }
		       });
    }
	
	/**
	 * Trivial constructor.
	 */
	public AdjVertexEdge() {
	}	
	
	/**
	 * Constructor with opposite vertex.
	 * @param opposite opposite vertex's id.
	 */
	public AdjVertexEdge(String opposite) {
		this.opposite=opposite;
	}
	
	/**
	 * Copy constructor.
	 * @param adjVertexEdge
	 */
	public AdjVertexEdge(AdjVertexEdge adjVertexEdge) {
		this.opposite=adjVertexEdge.opposite;
	}
	/**
	 * Get the id of the vertex on the other end of this edge.
	 * @return
	 */
	public String getOpposite() {
		return opposite;
	}
	/**
	 * Set the id of the vertex on the other end.
	 * @param opposite
	 */
	public void setOpposite(String opposite) {
		this.opposite = opposite;
	}
	
	public String toString() {
		return opposite;
	}
	
  @Override
  public void fromString(String encoding) {
    opposite = encoding;
  }
  	public Object clone() {
		return new AdjVertexEdge(this);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.opposite = Text.readString(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		Text.writeString(out, opposite);
	}

	@Override
	public Iterator<? extends Element> getIncidentElements() {
	  /**
	   * Internal class used to implement customized iterator logic.
	   */
		class Itr implements Iterator<Vertex> {
			Iterator<Vertex> itr;
			
			public Itr(Iterator<Vertex> itr) {
				this.itr=itr;
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
				throw new UnsupportedOperationException("This is a read-only iterator");
			}
		}
		// Only iterator on the id of the opposite vertex.
		List<Vertex> vertexs=new ArrayList<Vertex>();
		vertexs.add(new Vertex(opposite));
		
		Itr itr=new Itr(vertexs.iterator());
		
		return itr;
	}

  @Override
  public boolean equals(Object obj) {
    if(obj instanceof AdjVertexEdge){
      return opposite.equals(((AdjVertexEdge) obj).getOpposite());
    }else{
      return false;
    }
  }

  @Override
  public int hashCode() {
    return opposite.hashCode();
  }


}
