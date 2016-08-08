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

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableFactories;
import org.apache.hadoop.io.WritableFactory;
import org.sf.xrime.model.Element;
import org.sf.xrime.model.edge.AbstractEdge;


/**
 * Base class for vertexes in a graph.
 * @author xue
 */
public class Vertex implements Element, Cloneable {
  /**
   * Each vertex is identified with a string id.
   */
	protected String id = null;

	static {
	  // Register the writable factory for this class.
		WritableFactories.setFactory(Vertex.class, new WritableFactory() {
			public Writable newInstance() {
				return new Vertex();
			}
		});
	}
	/**
	 * Default constructor.
	 */
	public Vertex() {

	}
	/**
	 * Normal constructor.
	 * @param id
	 */
	public Vertex(String id) {
		this.id = id;
	}
	/**
	 * Copy constructor.
	 * @param vertex
	 */
	public Vertex(Vertex vertex) {
		this.id = vertex.getId();
	}
	/**
	 * Get the id of this vertex.
	 * @return
	 */
	public String getId() {
		return id;
	}
	/**
	 * Set the id of this vertex.
	 * @param id
	 */
	public void setId(String id) {
		this.id = id;
	}

	public String toString() {
		return id;
	}
	
  @Override
  public void fromString(String encoding) {
    this.id = encoding;
  }
	public Object clone() {
		return new Vertex(this);
	}

	public void readFields(DataInput in) throws IOException {
		this.id = Text.readString(in);
	}

	public void write(DataOutput out) throws IOException {
		Text.writeString(out, id);
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof Vertex) {
			return id.equals(((Vertex) obj).getId());
		} else {
			return false;
		}
	}

	@Override
	public int hashCode() {
		return id.hashCode();
	}

	/* 
	 * Since we do not really own any incident element, so hasNext() is always return false.
	 * @see org.sf.xrime.model.Element#getIncidentElements()
	 */
	@Override
	public Iterator<AbstractEdge>  getIncidentElements() {
	  /**
	   * Use an internal class to implement customized iterator logic.
	   */
		class Itr implements Iterator<AbstractEdge> {
			@Override
			public boolean hasNext() {
				return false;
			}

			@Override
			public AbstractEdge next() {
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
