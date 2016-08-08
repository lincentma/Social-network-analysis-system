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
 * Directed edge with both ends specified.
 */
public class Edge extends AbstractEdge implements Element {
  /**
   * The from end of this edge, i.e., the id of the vertex from which
   * this edge goes out.
   */
	protected String from = null;
	/**
	 * The to end of this edge, i.e., the id of the vertex to which this
	 * edge comes into.
	 */
	protected String to = null;

  private static String nullString = "";

	static {
	  // Register the writable factory for this class.
		WritableFactories.setFactory(Edge.class, new WritableFactory() {
			public Writable newInstance() {
				return new Edge();
			}
		});
	}
	/**
	 * The default constructor.
	 */
	public Edge() {
	}
	/**
	 * Normal constructor.
	 * @param from
	 * @param to
	 */
	public Edge(String from, String to) {
		this.from = from;
		this.to = to;
	}
	/**
	 * Copy constructor.
	 * @param edge
	 */
	public Edge(Edge edge) {
		this(edge.getFrom(), edge.getTo());
	}
	/**
	 * Get the id of the from end of this edge.
	 * @return
	 */
	public String getFrom() {
		return from;
	}
	/**
	 * Set the from end of this edge.
	 * @param from the id of the from end vertex.
	 */
	public void setFrom(String from) {
		this.from = from;
	}
	/**
	 * Get the id of the to end of this edge.
	 * @return
	 */
	public String getTo() {
		return to;
	}
	/**
	 * Set the id of the to end of this edge.
	 * @param to
	 */
	public void setTo(String to) {
		this.to = to;
	}

	@Override
	public String toString() {
	  StringBuffer buffer = new StringBuffer();
	  buffer.append("<");
	  if(from!=null){
	    buffer.append(from);
	  }
	  buffer.append(", ");
	  if(to!=null){
	    buffer.append(to);
	  }
	  buffer.append(">");
	  return buffer.toString();
	}
	
  @Override
  public void fromString(String encoding) {
    // Clean.
    this.from = null;
    this.to = null;
    
    if(encoding.length()<=3) return;
    
    int comma_index = encoding.indexOf(", ");
    this.from = encoding.substring(1, comma_index);
    this.to = encoding.substring(comma_index+2,encoding.length()-1);
  }
	public Object clone() {
		return new Edge(this);
	}

	public void readFields(DataInput in) throws IOException {
		this.from = Text.readString(in);
		if (from.length() == 0) {
			from = null;
		}

		this.to = Text.readString(in);
		if (to.length() == 0) {
			to = null;
		}
	}

	public void write(DataOutput out) throws IOException {
		if (from == null) {
			Text.writeString(out, nullString);
		} else {
			Text.writeString(out, from);
		}

		if (to == null) {
			Text.writeString(out, nullString);
		} else {
			Text.writeString(out, to);
		}
	}

	@Override
	public Iterator<? extends Element> getIncidentElements() {
	  /**
	   * Internal class to implement customized iterator logic.
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
		// The incident elements of this edge is its from end and to end.
		List<Vertex> vertexs=new ArrayList<Vertex>();
		vertexs.add(new Vertex(from));
		vertexs.add(new Vertex(to));
		
		Itr itr=new Itr(vertexs.iterator());
		
		return itr;
	}
	/**
	 * Utility function, used to convert AdjVertexEdge to Edge object.
	 * The input AdjVertexEdge object specifies the from end of newly
	 * created Edge object.
	 * @param edge 
	 * @param to specifies the to end of newly created edge.
	 * @return
	 */
	public static Edge convertToEdge(AdjVertexEdge edge, String to) {
		if(edge==null) {
			return null;
		}
		
		Edge ret;
		
		ret=new Edge(edge.getOpposite(), to);
		// Labelable edges could be recognized and processed accordingly.
		if(edge instanceof LabeledAdjVertexEdge) {
			LabeledEdge labeledEdge=new LabeledEdge(ret);
			labeledEdge.setLabels(((LabeledAdjVertexEdge) edge).getLabels());
			
			ret=labeledEdge;
		} 
		
		return ret;
	}
	/**
	 * Similar with above method. The input AdjVertexEdge object specifies
	 * the to end of newly created Edge object.	
	 * @param from the from end of the newly created Edge.
	 * @param edge
	 * @return
	 */
	public static Edge convertToEdge(String from ,AdjVertexEdge edge) {
		if(edge==null) {
			return null;
		}
		
		Edge ret;
		
		ret=new Edge(from, edge.getOpposite());
		
		if(edge instanceof LabeledAdjVertexEdge) {
			LabeledEdge labeledEdge=new LabeledEdge(ret);
			labeledEdge.setLabels(((LabeledAdjVertexEdge) edge).getLabels());
			
			ret=labeledEdge;
		} 
		
		return ret;
	}
	
  @Override
  public boolean equals(Object obj) {
	  if(obj instanceof Edge){
	    return this.from.equals(((Edge) obj).getFrom()) && 
	           this.to.equals(((Edge) obj).getTo());
	  }else{
	    return false;
	  }
  }
  @Override
  public int hashCode() {
    return (from+to).hashCode();
  }

}
