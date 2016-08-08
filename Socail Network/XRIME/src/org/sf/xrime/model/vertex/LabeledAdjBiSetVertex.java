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
 * An object of this class represents a vertex with forward/backward adjacent
 * vertexes and some labels.
 * 
 * @author xue
 */
public class LabeledAdjBiSetVertex extends AdjBiSetVertex implements Labelable {
  /**
   * Labels of this vertex.
   */
	private Labels _labels = null;

	static {
	  // Register the writable factory for this class.
		WritableFactories.setFactory(LabeledAdjBiSetVertex.class,
				new WritableFactory() {
					public Writable newInstance() {
						return new LabeledAdjBiSetVertex();
					}
				});
	}

	/**
	 * Default constructor.
	 */
	public LabeledAdjBiSetVertex() {
		super();
		_labels = new Labels();
	}
	
	/**
	 * Another constructor.
	 * @param id
	 */
	public LabeledAdjBiSetVertex(String id){
	  super(id);
	  _labels = new Labels();
	}

	/**
	 * Copy constructor.
	 * 
	 * @param copy
	 */
	public LabeledAdjBiSetVertex(LabeledAdjBiSetVertex copy) {
		super(copy);
		_labels=new Labels(copy._labels);
	}
	

	/**
	 * Downcast constructor.
	 * 
	 * @param copy
	 */
	public LabeledAdjBiSetVertex(AdjBiSetVertex vertex) {
		super(vertex);
		_labels=new Labels();
	}

	@Override
	public Object clone() {
		return new LabeledAdjBiSetVertex(this);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// Deal with fields of super.
		super.readFields(in);
		_labels.readFields(in);
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
    _backward_vertexes.clear();
    _forward_vertexes.clear();
    _labels.clearLabels();
    
    // Find the delimiter of AdjSetVertex string and labels string.
    int pointerA = 0;
    // Move beyond the id.
    // Move to the end of forward opposites.
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
    
    // Move to the end of backward opposites.
    pointerA += 3;
    bracket_num = 1;
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
    String opps_str = encoding.substring(0, pointerA) + ">";
    String labels_str = encoding.substring(pointerA+2, encoding.length()-1);
    
    // Parse them.
    super.fromString(opps_str);
    _labels.fromString(labels_str);	}

	@Override
	public void write(DataOutput out) throws IOException {
		// Deal with fields of super.
		super.write(out);
		_labels.write(out);
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
	 * Get the label with specified name.
	 * 
	 * @param name
	 * @return
	 */
	public String getStringLabel(String name) {
		return _labels.getStringLabel(name);
	}

	/**
	 * Add or modify a label.
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
}
