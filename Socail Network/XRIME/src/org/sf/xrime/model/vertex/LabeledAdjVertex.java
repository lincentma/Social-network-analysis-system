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

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableFactories;
import org.apache.hadoop.io.WritableFactory;
import org.sf.xrime.model.label.Labelable;
import org.sf.xrime.model.label.Labels;


/**
 * AdjVertex with labels.
 */
public class LabeledAdjVertex extends AdjVertex implements Labelable {
  /**
   * Internal container for labels of this vertex.
   */
	private Labels _labels = null;
	
	static {
	  // Register writable factory for this class.
		WritableFactories.setFactory(LabeledAdjVertex.class, new WritableFactory() {
			public Writable newInstance() {
				return new LabeledAdjVertex();
			}
		});
	}
	/**
	 * Default constructor.
	 */
	public LabeledAdjVertex() {
	  super();
	  _labels = new Labels();
	}
	/**
	 * Normal constructor.
	 * @param id
	 */
	public LabeledAdjVertex(String id) {
		super(id);
		_labels = new Labels();
	}
	/**
	 * Yet another constructor.
	 * @param adjVertex
	 */
	public LabeledAdjVertex(AdjVertex adjVertex) {
		super(adjVertex);
		_labels = new Labels();
	}
	/**
	 * Copy constructor.
	 * @param labeledAdjVertex
	 */
	public LabeledAdjVertex(LabeledAdjVertex labeledAdjVertex) {
		super(labeledAdjVertex);
		_labels=(Labels) labeledAdjVertex._labels.clone();
	}
	
	@Override
	public Writable getLabel(String name) {
		return _labels.getLabel(name);
	}

	@Override
	public void setLabel(String name, Writable value) {
		_labels.setLabel(name, value);
	}

	@Override
	public void removeLabel(String name) {
		_labels.removeLabel(name);
	}


	/**
	 * Get the string-valued label with specified name.
	 * 
	 * @param name
	 * @return
	 */
	public String getStringLabel(String name) {
		return _labels.getStringLabel(name);
	}

	/**
	 * Add or modify a string-valued label.
	 * 
	 * @param name
	 * @param value
	 */
	public void setStringLabel(String name, String value) {
		_labels.setStringLabel(name, value);
	}

  @Override
  public void clearLabels() {
    _labels.clearLabels();
  }

  @Override
	public String toString() {
	  StringBuffer buffer = new StringBuffer();
	  String super_str = super.toString();
		buffer.append(super_str.substring(0, super_str.length()-1));
		buffer.append(", ");
		buffer.append(_labels.toString());
		buffer.append(">");
		return buffer.toString();
	}
  
  @Override
  public void fromString(String encoding){
    // Clean.
    id = null;
    edges.clear();
    _labels.clearLabels();
    
    // Find the delimiter of AdjVertex string and labels string.
    int pointerA = 0;
    // Move beyond the id.
    pointerA = encoding.indexOf(", <", 0);
    pointerA += 3;
    int bracket_num = 1;
    while(pointerA<encoding.length()&&bracket_num!=0){
      if(encoding.charAt(pointerA)=='<'){
        bracket_num++;
      }else if(encoding.charAt(pointerA)=='>'){
        bracket_num--;
      }
      pointerA++;
    }
    
    // Shouldn't happen.
    if(pointerA==encoding.length()) return;
    
    // Get the two strings.
    String edges_str = encoding.substring(0, pointerA) + ">";
    String labels_str = encoding.substring(pointerA+2, encoding.length()-1);
    
    // Parse them.
    super.fromString(edges_str);
    _labels.fromString(labels_str);
  }

	public Object clone() {
		return new LabeledAdjVertex(this);
	}

	public void readFields(DataInput in) throws IOException {
		super.readFields(in);
		_labels.readFields(in);
	}

	public void write(DataOutput out) throws IOException {
		super.write(out);
		_labels.write(out);
	}
}
