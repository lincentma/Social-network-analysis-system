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
package org.sf.xrime.model.label;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableFactories;

/**
 * An utility class to be aggregated within bigger classes to implement Labelable
 * interface.
 */
public class Labels implements Labelable, Writable, Cloneable {
	/**
	 * The internal container of key-value pairs.
	 */
	private Map<String, Writable> labels;
	/**
	 * Default constructor.
	 */
	public Labels() {
		labels=new HashMap<String, Writable>();
	}
	/**
	 * Copy constructor. Not deep copy constructor. We assume values (Writables)
	 * in the hash map could be safely reused and shared, like String. However,
	 * a Writable is not necessarily unchangable. So, this is only assumptions.
	 * @param lable
	 */
	public Labels(Labels lable) {
		this.labels=new HashMap<String, Writable>();
		this.labels.putAll(lable.getLabels());		
	}
	/**
	 * Retrieve all lables as a map.
	 * @return
	 */
	public Map<String, Writable> getLabels() {
		return labels;
	}
	/**
	 * Replace all labels with those stored in the specified map.
	 * @param labels
	 */
	public void setLabels(Map<String, Writable> labels) {
		this.labels = labels;
	}

	@Override
	public Writable getLabel(String name) {
		return labels.get(name);
	}

	@Override
	public void removeLabel(String name) {
		labels.remove(name);
	}

	@Override
	public void clearLabels() {
		labels.clear();
	}

	@Override
	public void setLabel(String name, Writable value) {
		labels.put(name, value);
	}
	/**
	 * Handy method used to get a label with string value. If the
	 * label is not found, return null instead of throwing exceptions.
	 * @param name
	 * @return
	 */
	public String getStringLabel(String name) {
		Writable writable=labels.get(name);
		if(writable instanceof Text) {
			return ((Text) writable ).toString(); 
		}

		return null;
	}
	/**
	 * Handy method used to put a label with string value.
	 * @param name
	 * @param value
	 */
	public void setStringLabel(String name, String value) {
		labels.put(name, new Text(value));
	}
	/**
	 * Handy method used to get a label with integer value. If the label
	 * is not found, throw exception.
	 * @param name
	 * @return
	 * @throws NoSuchKeyException
	 */
	public int getIntLabel(String name) throws NoSuchKeyException {
		Writable writable=labels.get(name);
		if(writable instanceof IntWritable) {
			return ((IntWritable)writable).get(); 
		}

		throw new NoSuchKeyException("No Such Key: "+name);
	}
	/**
	 * Handy method used to get a label with integer value. If the label
	 * is not found, return specified default value instead of throwing
	 * exceptions.
	 * @param name
	 * @param defaultValue
	 * @return
	 */
	public int getIntLabel(String name, int defaultValue){
		Writable writable=labels.get(name);
		if(writable instanceof IntWritable) {
			return ((IntWritable) writable ).get(); 
		}

		return defaultValue;
	}
	/**
	 * Handy method used to put a label with integer value.
	 * @param name
	 * @param value
	 */
	public void setIntLabel(String name, int value) {
		labels.put(name, new IntWritable(value));
	}

	public String toString() {
		if(labels.size()==0) {
			return "<>";
		}

		String ret="<";
		for( String key : labels.keySet() ) {
			ret+="<" + key + ", " + labels.get(key).toString() + ">, ";
		}

		return ret.substring(0, ret.length() - 2)+">";
	}
	
	/**
	 * This is added for HadoopML, which does not have the capability to deal with Writable now.
	 * Actually, X-RIME choose a totally different, more expressive and more extendable data
	 * model than HadoopML. As a natural result, some X-RIME data model features have to be
	 * restricted before integrating with HadoopML. Here is a case. Since it's awkward and blurry
	 * to determine label value type from its string encoding, we choose to assume all of them
	 * are strings and left space for latter processing by applications which know the real type.
	 * 
	 * @param encoding
	 */
	public void fromString(String encoding){
	  labels.clear();
	  // Skip the empty case.
	  if (encoding.length()==2){
	    return;
	  }
	  // The content of all labels.
	  String content_str = encoding.substring(1,encoding.length()-1);
	  int last_pair_start_index = 0;
	  int pair_delim_index = 0;
	  while(true){
	    pair_delim_index = content_str.indexOf(">, <", last_pair_start_index);
	    if(pair_delim_index==-1){
	      // Reach the end of the string.
	      String pair_str = content_str.substring(last_pair_start_index, content_str.length());
	      int kv_delim_index = pair_str.indexOf(", ");
	      String k = pair_str.substring(1, kv_delim_index);
	      String v = pair_str.substring(kv_delim_index+2, pair_str.length()-1);
	      labels.put(k, new Text(v));
	      return;
	    }else{
	      // In the middle of the string.
	      String pair_str = content_str.substring(last_pair_start_index, pair_delim_index+1);
	      int kv_delim_index = pair_str.indexOf(", ");
	      String k = pair_str.substring(1, kv_delim_index);
	      String v = pair_str.substring(kv_delim_index+2, pair_str.length()-1);
	      labels.put(k, new Text(v));
	    }
	    // Move forward the pointer.
	    last_pair_start_index = pair_delim_index+3;
	  }
	}

	public Object clone() {
		return new Labels(this);
	}

	@Override
	@SuppressWarnings("unchecked")
	public void readFields(DataInput in) throws IOException {
		// Clear the container.
		labels.clear();
		// Determine the size of container.
		int size=in.readInt();
		while( size-- > 0) {
			// Each key is a string, but the label value may be of different types.
			String key=Text.readString(in);
			String valueClassName=Text.readString(in);

			try {
				Class instanceClass;
				instanceClass = Class.forName(valueClassName);
				Writable writable = WritableFactories.newInstance(instanceClass, null);
				writable.readFields(in);
				labels.put(key, writable);
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
				throw new IOException(e.getMessage());
			}
		}		
	}

	@Override
	public void write(DataOutput out) throws IOException {
		if(labels==null) {
			out.writeInt(0);
			return;
		}

		out.writeInt(labels.size());
		for(String key : labels.keySet()) {
			Text.writeString(out, key);
			Writable value=labels.get(key);
			Text.writeString(out, value.getClass().getName());
			value.write(out);
		}
	}
}
