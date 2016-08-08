/*
 * Copyright (C) yangyin@BUPT. 2009.
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

import org.apache.hadoop.io.Writable;
/**
 * 
 * @author yangyin
 *	This class is used to indicate the weight of a weighted edge
 */
public class WeightOfEdge implements Cloneable, Writable {

	private double weight;
	
	public WeightOfEdge(){
		weight = 0;
	}
	
	public WeightOfEdge(double weight){
		this.weight = weight;
	}
	public WeightOfEdge(WeightOfEdge weightOfEdge){
		this.weight = weightOfEdge.getWeight();
	}

	public double getWeight() {
		return weight;
	}

	public void setWeight(double weight) {
		this.weight = weight;
	}
	
	public Object clone() {
		return new WeightOfEdge(this);
	}
	
	public String toString(){
		String ret = "";
		ret = ret + weight;
		return ret;
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		// Clear the container.
		weight = in.readDouble();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeDouble(weight);
	}
}
