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
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableFactories;
import org.apache.hadoop.io.WritableFactory;
import org.sf.xrime.model.edge.AbstractEdge;
import org.sf.xrime.model.edge.AdjVertexEdge;


/**
 * An object of this class represents a vertex and its bi-directional adjacent
 * vertexes. The difference with AdjSetVertex is that this class distinguishes
 * the forward adjacent vertexes and backward adjacent vertexes.
 * 
 * @author xue
 * 
 */
public class AdjBiSetVertex extends Vertex implements Cloneable {
	static {
	  // Register the writable factory for this class.
		WritableFactories.setFactory(AdjBiSetVertex.class,
				new WritableFactory() {
					public Writable newInstance() {
						return new AdjBiSetVertex();
					}
				});
	}

	/**
	 * The set for forward adjacent vertexes. All elements should have the same
	 * type (AdjVertexEdge or its derivants).
	 */
	protected Set<AdjVertexEdge> _forward_vertexes = null;

	/**
	 * The set for backward adjacent vertexes. All elements should have the same
	 * type (AdjVertexEdge or its derivants).
	 */
	protected Set<AdjVertexEdge> _backward_vertexes = null;

	/**
	 * Default constructor.
	 */
	public AdjBiSetVertex() {
		super();
		_forward_vertexes = new HashSet<AdjVertexEdge>();
		_backward_vertexes = new HashSet<AdjVertexEdge>();
	}
	
	/**
	 * Another constructor.
	 * @param id
	 */
	public AdjBiSetVertex(String id){
	  super(id);
		_forward_vertexes = new HashSet<AdjVertexEdge>();
		_backward_vertexes = new HashSet<AdjVertexEdge>();
	}

	/**
	 * Copy constructor.
	 * 
	 * @param copy
	 */
	public AdjBiSetVertex(AdjBiSetVertex copy) {
		super(copy);
		_forward_vertexes = new HashSet<AdjVertexEdge>();
		for(AdjVertexEdge edge: copy._forward_vertexes){
		  _forward_vertexes.add((AdjVertexEdge) edge.clone());
		}
		_backward_vertexes = new HashSet<AdjVertexEdge>();
		for(AdjVertexEdge edge: copy._backward_vertexes){
		  _backward_vertexes.add((AdjVertexEdge) edge.clone());
		}
	}

	/**
	 * Get the set of forward vertexes of this vertex.
	 * 
	 * @return
	 */
	public Set<AdjVertexEdge> getForwardVertexes() {
		return _forward_vertexes;
	}

	/**
	 * Get the set of backward vertexes of this vertex.
	 * 
	 * @return
	 */
	public Set<AdjVertexEdge> getBackwardVertexes() {
		return _backward_vertexes;
	}

	/**
	 * Add specified vertex to the forward vertexes set.
	 * 
	 * @param vertex
	 */
	public void addForwardVertex(AdjVertexEdge vertex) {
		_forward_vertexes.add(vertex);
	}

	/**
	 * Remove specified vertex from the forward vertexes set.
	 * 
	 * @param vertex
	 */
	public void removeForwardVertex(AdjVertexEdge vertex) {
		_forward_vertexes.remove(vertex);
	}

	/**
	 * Clear the set of forward vertex set.
	 */
	public void clearForwardVertex() {
		_forward_vertexes.clear();
	}

	/**
	 * Add specified vertex to the backward vertexes set.
	 * 
	 * @param vertex
	 */
	public void addBackwardVertex(AdjVertexEdge vertex) {
		_backward_vertexes.add(vertex);
	}

	/**
	 * Remove specified vertex from the backward vertexes set.
	 * 
	 * @param vertex
	 */
	public void removeBackwardVertex(AdjVertexEdge vertex) {
		_backward_vertexes.remove(vertex);
	}

	/**
	 * Clear the set of backward vertex set.
	 */
	public void clearBackwardVertex() {
		_backward_vertexes.clear();
	}

	/**
	 * Remove loop.
	 */
	public void removeLoop() {
		_forward_vertexes.remove(id);
		_backward_vertexes.remove(id);
	}

	@Override
	public Object clone() {
		return new AdjBiSetVertex(this);
	}

	@SuppressWarnings("unchecked")
  @Override
	public void readFields(DataInput in) throws IOException {
		// Call super first.
		super.readFields(in);
		// Clean containers.
		_forward_vertexes.clear();
		_backward_vertexes.clear();
		// Deal with forward vertex set.
		int size = in.readInt();
		if(size>0){
		  // Determine the type of elements.
			String className = Text.readString(in);
			try {
        Class instanceClass = Class.forName(className);
				for (int ii = 0; ii < size; ii++) {
          Writable writable = WritableFactories
              .newInstance(instanceClass, null);
          writable.readFields(in);
          if (writable instanceof AdjVertexEdge) {
            addForwardVertex((AdjVertexEdge) writable);
          }
        }      } catch (ClassNotFoundException e) {
        e.printStackTrace();
      }
		}
		// Deal with backward vertex set.
		size = in.readInt();
		if(size>0){
		  // Determine the type of elements.
		  String className = Text.readString(in);
		  Class instanceClass;
      try {
        instanceClass = Class.forName(className);
        for(int i=0;i<size;i++){
          Writable writable = WritableFactories
            .newInstance(instanceClass, null);
          writable.readFields(in);
          if(writable instanceof AdjVertexEdge){
            addBackwardVertex((AdjVertexEdge) writable);
          }
        }
      } catch (ClassNotFoundException e) {
        e.printStackTrace();
      }
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// Write the pure vertex id.
		super.write(out);
		// Deal with the forward and backward vertexes sets.
		if (_forward_vertexes == null || _forward_vertexes.size() == 0) {
			out.writeInt(0);
		} else {
			out.writeInt(_forward_vertexes.size());
			Text.writeString(out, _forward_vertexes.toArray()[0].getClass().getName());
			for (AdjVertexEdge edge : _forward_vertexes) {
			  edge.write(out);
			}
		}
		if (_backward_vertexes == null || _backward_vertexes.size() == 0) {
			out.writeInt(0);
		} else {
			out.writeInt(_backward_vertexes.size());
			Text.writeString(out, _backward_vertexes.toArray()[0].getClass().getName());
			for (AdjVertexEdge edge : _backward_vertexes) {
			  edge.write(out);
			}
		}
	}

	@Override
	public String toString() {
		StringBuffer result_buf = new StringBuffer();
		result_buf.append("<");
		result_buf.append(id);
		result_buf.append(", <");
		for (AdjVertexEdge edge : _forward_vertexes) {
			result_buf.append(((AdjVertexEdge)edge).toString());
			result_buf.append(", ");
		}
		if (_forward_vertexes.size() > 0)
			result_buf.delete(result_buf.length() - 2, result_buf.length());
		result_buf.append(">, <");
		for (AdjVertexEdge edge : _backward_vertexes) {
			result_buf.append(((AdjVertexEdge)edge).toString());
			result_buf.append(", ");
		}
		if (_backward_vertexes.size() > 0)
			result_buf.delete(result_buf.length() - 2, result_buf.length());
		result_buf.append(">>");
		return result_buf.toString();
	}
	
	@Override
	public void fromString(String encoding){
	  // Clean.
	  id = null;
	  _forward_vertexes.clear();
	  _backward_vertexes.clear();
	  
	  // id.
	  int pointerA = 0, pointerB = 0;
	  pointerB = encoding.indexOf(", <", pointerA);
	  id = encoding.substring(pointerA+1, pointerB);
	  pointerA = pointerB+3;
	  
	  // forward and backward opposite vertexes.
	  pointerB = encoding.indexOf(">, <", pointerA);
	  String forward_str = encoding.substring(pointerA, pointerB);
	  String backward_str = encoding.substring(pointerB+4, encoding.length()-2);
	  
	  if(forward_str.length()!=0){
	    pointerA=0;
	    pointerB=0;
	    while(true){
	      pointerB = forward_str.indexOf(", ",pointerA);
	      if(pointerB==-1){
	        _forward_vertexes.add(new AdjVertexEdge(forward_str.substring(pointerA, forward_str.length())));
	        break;
	      }else{
	        _forward_vertexes.add(new AdjVertexEdge(forward_str.substring(pointerA, pointerB)));
	      }
	      pointerA = pointerB + 2;
	    }	    
	  }

	  if(backward_str.length()!=0){
	    pointerA=0;
	    pointerB=0;
	    while(true){
	      pointerB = backward_str.indexOf(", ", pointerA);
	      if(pointerB==-1){
	        _backward_vertexes.add(new AdjVertexEdge(backward_str.substring(pointerA, backward_str.length())));
	        break;
	      }else{
	        _backward_vertexes.add(new AdjVertexEdge(backward_str.substring(pointerA, pointerB)));
	      }
	      pointerA = pointerB + 2;
	    }
	  }
	}
	
	@Override
	public Iterator<AbstractEdge>  getIncidentElements() {
		class Itr implements Iterator<AbstractEdge> {
			int status=0;
			Iterator<AdjVertexEdge> itrForward;
			Iterator<AdjVertexEdge> itrBackward;
						
			@Override
			public boolean hasNext() {
        boolean ret = false;

        if (status == 0) {
          if (itrForward != null) {
            status = 1;
            return itrForward.hasNext();
          }
          if (itrBackward != null) {
            status = 2;
            return itrBackward.hasNext();
          }
          status = 3;
          return false;
        } else if (status == 1) {
          ret = itrForward.hasNext();
          if (!ret) {
            if (itrBackward != null) {
              status = 2;
              return itrBackward.hasNext();
            }
            status = 3;
            return false;
          }
        } else if (status == 2) {
          ret = itrBackward.hasNext();
          if (!ret) {
            status = 3;
            return false;
          }
        } else if (status == 3) {
          return false;
        }

        return ret;
      }

			@Override
			public AbstractEdge next() {
				if(status==1) {
					return itrForward.next();
				} else if (status==2) {
					return itrBackward.next();
				}
				return null;
			}

			@Override
			public void remove() {
				throw new UnsupportedOperationException("This is a read-only iterator");
			}

			public void setItrForward(Iterator<AdjVertexEdge> itr) {
				this.itrForward = itr;
			}	
			
			public void setItrBackward(Iterator<AdjVertexEdge> itr) {
				this.itrBackward = itr;
			}	
		}
		
		Itr ret=new Itr();
		
		if(_forward_vertexes!=null) {
			ret.setItrForward(_forward_vertexes.iterator());			
		}
		if(_backward_vertexes!=null) {
			ret.setItrBackward(_backward_vertexes.iterator());			
		}
		
		return ret;
	}
}
