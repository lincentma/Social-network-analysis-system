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
import org.sf.xrime.model.Element;

/**
 * Set of VertexSets.
 * @author xue
 */
public class SetOfVertexSets implements Element, Cloneable  {
  /**
   * Internal data structure.
   */
  private Set<VertexSet> _the_set = null;
  
	static {
	  // Register the writable factory for this class.
		WritableFactories.setFactory(SetOfVertexSets.class, new WritableFactory() {
			public Writable newInstance() {
				return new SetOfVertexSets();
			}
		});
	}
	
	/**
	 * Default constructor.
	 */
	public SetOfVertexSets(){
	  _the_set = new HashSet<VertexSet>();
	}
	/**
	 * Copy constructor.
	 * @param set
	 */
	public SetOfVertexSets(SetOfVertexSets set){
	  _the_set = new HashSet<VertexSet>();
	  for(VertexSet in_set : set._the_set){
	    _the_set.add(new VertexSet(in_set));
	  }
	}
	
	/**
	 * Get the set of vertex sets.
	 * @return
	 */
	public Set<VertexSet> getVertexSets(){
	  return _the_set;
	}
	/**
	 * Specify the set of vertex sets.
	 * @param set
	 */
	public void setVertexSets(Set<VertexSet> set){
	  _the_set = set;
	}
	/**
	 * Add a vertex set.
	 * @param set
	 */
	public void addVertexSet(VertexSet set){
	  _the_set.add(set);
	}
	/**
	 * Add specified vertex sets.
	 * @param set
	 */
	public void addVertexSets(Set<VertexSet> set){
	  _the_set.addAll(set);
	}
	/**
	 * Clear.
	 */
	public void clear(){
	  _the_set.clear();
	}
	
  @Override
  protected Object clone() throws CloneNotSupportedException {
    return new SetOfVertexSets(this);
  }
  
  @Override
  public String toString() {
		StringBuffer result_buf = new StringBuffer();
		result_buf.append("(");
		for (VertexSet vertex_set : _the_set) {
			result_buf.append(vertex_set.toString());
			result_buf.append(", ");
		}
		if (_the_set.size() > 0)
			result_buf.delete(result_buf.length() - 2, result_buf.length());
		result_buf.append(")");
		return result_buf.toString();
  }
  
  @Override
  public void fromString(String encoding){
    // Clean.
    _the_set.clear();
    
    String sets_str = encoding.substring(1, encoding.length()-1);
    
    // Empty set of sets.
    if(sets_str.length()==0) return;
    
    int pointerA = 0, pointerB = 0;
    while(true){
      pointerB = sets_str.indexOf("), (", pointerA);
      if(pointerB==-1){
        String set_str = sets_str.substring(pointerA, sets_str.length());
        VertexSet temp_set = new VertexSet();
        temp_set.fromString(set_str);
        _the_set.add(temp_set);
        return;
      }else{
        String set_str = sets_str.substring(pointerA, pointerB+1);
        VertexSet temp_set = new VertexSet();
        temp_set.fromString(set_str);
        _the_set.add(temp_set);      }
      pointerA = pointerB + 3;
    }
  }
  
  @SuppressWarnings("unchecked")
  @Override
  public void readFields(DataInput in) throws IOException {
	  // Clear the container.
		_the_set.clear();
		// Determine the size.
		int size = in.readInt();
		if (size > 0) {
		  // All vertex sets in the set should have the same type.
			String className = Text.readString(in);
			try {
				Class instanceClass;
				instanceClass = Class.forName(className);
				for (int i = 0; i < size; i++) {
					Writable writable = WritableFactories.newInstance(
							instanceClass, null);
					writable.readFields(in);
					if (writable instanceof VertexSet) {
						addVertexSet((VertexSet) writable);
					}
				}
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			}
		}
  }

  @Override
  public void write(DataOutput out) throws IOException {
		if (_the_set == null || _the_set.size() == 0) {
			out.writeInt(0);
		} else {
		  // Write the number of vertex sets in this set.
			out.writeInt(_the_set.size());
			// All the vertex sets should have the same type.
			Text.writeString(out, _the_set.toArray()[0].getClass().getName());
			for (VertexSet vertex_set : _the_set) {
				vertex_set.write(out);
			}
		}
  }
  
  @Override
  public Iterator<? extends Element> getIncidentElements() {
    return null;
  }
}
