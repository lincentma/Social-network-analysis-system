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
package org.sf.xrime.model.path;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableFactories;
import org.apache.hadoop.io.WritableFactory;
import org.sf.xrime.model.Element;
import org.sf.xrime.model.vertex.Vertex;


/**
 * This is a representation of path based on list of vertexes. The order of vertexes within
 * the path is from the root to the leaf.
 * @author xue
 */
public class PathAsVertexesList implements Element, Cloneable {
  /**
   * Internal list. All its elements should have the same type.
   */
	private List<Vertex> _vertexes = null;

	static {
	  // Register the writable factory for this class.
		WritableFactories.setFactory(PathAsVertexesList.class, new WritableFactory() {
			public Writable newInstance() {
				return new PathAsVertexesList();
			}
		});
	}

	/**
	 * Basic constructor.
	 */
	public PathAsVertexesList() {
		_vertexes = new LinkedList<Vertex>();
	}

	/**
	 * Copy constructor.
	 * @param list 
	 */
	public PathAsVertexesList(PathAsVertexesList list) {
		_vertexes = new LinkedList<Vertex>();
		for (int i=0;i<list._vertexes.size();i++) {
			_vertexes.add((Vertex) list._vertexes.get(i).clone());
		}
	}
	/**
	 * Get the list of vertexes.
	 * @return
	 */
	public List<Vertex> getVertexes() {
		return _vertexes;
	}
	/**
	 * Replace internal list with specified list.
	 * @param set
	 */
	public void setVertexes(List<Vertex> list) {
		_vertexes = list;
	}
	/**
	 * Add a vertex to this list.
	 * @param vertex
	 */
	public void addVertex(Vertex vertex) {
		_vertexes.add(vertex);
	}
	/**
	 * Add all vertexes in specified list to this list.
	 * @param set
	 */
	public void addVertexes(List<Vertex> list) {
		_vertexes.addAll(list);
	}
	/**
	 * Clear this vertex list.
	 */
	public void clear() {
		_vertexes.clear();
	}

	@Override
  public Object clone(){
		return new PathAsVertexesList(this);
	}

	@Override
	public String toString() {
		StringBuffer result_buf = new StringBuffer();
		result_buf.append("<");
		for (int i=0; i< _vertexes.size(); i++) {
		  // Cast to Vertex.
			result_buf.append(((Vertex)_vertexes.get(i)).toString());
			result_buf.append(", ");
		}
		if (_vertexes.size() > 0)
			result_buf.delete(result_buf.length() - 2, result_buf.length());
		result_buf.append(">");
		return result_buf.toString();
	}
	
  @Override
  public void fromString(String encoding) {
    // Clean.
    _vertexes.clear();
    
    String vertexes_str = encoding.substring(1, encoding.length()-1);
    // Empty path.
    if(vertexes_str.length()==0) return;
    int pointerA = 0, pointerB = 0;
    while(true){
      pointerB = vertexes_str.indexOf(", ", pointerA);
      if(pointerB==-1){
        Vertex new_vertex = new Vertex(vertexes_str.substring(pointerA, vertexes_str.length()));
        _vertexes.add(new_vertex);
        return;
      }else{
        Vertex new_vertex = new Vertex(vertexes_str.substring(pointerA, pointerB));
        _vertexes.add(new_vertex);
      }
      pointerA = pointerB + 2;
    }
  }

	@SuppressWarnings("unchecked")
	@Override
	public void readFields(DataInput in) throws IOException {
	  // Clear the container.
		_vertexes.clear();
		// Determine the size.
		int size = in.readInt();
		if (size > 0) {
		  // All vertexes in the set should have the same type.
			String className = Text.readString(in);
			try {
				Class instanceClass;
				instanceClass = Class.forName(className);
				for (int i = 0; i < size; i++) {
					Writable writable = WritableFactories.newInstance(
							instanceClass, null);
					writable.readFields(in);
					if (writable instanceof Vertex) {
						addVertex((Vertex) writable);
					}
				}
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			}
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {
		if (_vertexes == null || _vertexes.size() == 0) {
			out.writeInt(0);
		} else {
		  // Write the number of vertexes in this set.
			out.writeInt(_vertexes.size());
			// All the vertexes should have the same type.
			Text.writeString(out, _vertexes.toArray()[0].getClass().getName());
			for (int i=0; i<_vertexes.size(); i++) {
				_vertexes.get(i).write(out);
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
