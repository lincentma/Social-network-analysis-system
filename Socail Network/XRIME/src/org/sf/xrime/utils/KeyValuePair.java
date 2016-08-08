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
package org.sf.xrime.utils;

/**
 * Very simple Key-Value pairs wrapper.
 * @param <K> Key
 * @param <V> Value
 */
public class KeyValuePair<K, V> {
	private K key;
	private V value;
	
	/**
	 * Trivial constructor.
	 */
	public KeyValuePair() {			
	}
	
	/**
	 * Constructor by values.
	 * @param key
	 * @param value
	 */
	public KeyValuePair(K key, V value) {
		this.key=key;
		this.value=value;
	}

	public K getKey() {
		return key;
	}

	public void setKey(K key) {
		this.key = key;
	}

	public V getValue() {
		return value;
	}

	public void setValue(V value) {
		this.value = value;
	}
}
