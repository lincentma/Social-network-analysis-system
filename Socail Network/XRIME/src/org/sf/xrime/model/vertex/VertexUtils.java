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

/**
 * Utility class which provide utility methods for casting objects of different types.
 */
public class VertexUtils {
	static public Object getLabledVertex(Vertex vertex) {
		if(vertex instanceof AdjVertex) { 
			if(vertex instanceof LabeledAdjVertex) {
				return vertex;
			}
			else {
				return new LabeledAdjVertex((AdjVertex)vertex);
			}
		}

		if(vertex instanceof AdjSetVertex) { 
			if(vertex instanceof LabeledAdjSetVertex) {
				return vertex;
			}
			else {
				return new LabeledAdjSetVertex((AdjSetVertex)vertex);
			}
		}

		if(vertex instanceof AdjBiSetVertex) { 
			if(vertex instanceof LabeledAdjBiSetVertex) {
				return vertex;
			}
			else {
				return new LabeledAdjBiSetVertex((AdjBiSetVertex)vertex);
			}
		}
		
		return vertex;
	}
}
