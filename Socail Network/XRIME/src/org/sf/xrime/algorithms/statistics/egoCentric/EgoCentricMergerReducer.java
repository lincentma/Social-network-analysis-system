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
package org.sf.xrime.algorithms.statistics.egoCentric;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.sf.xrime.model.edge.Edge;
import org.sf.xrime.model.vertex.AdjVertex;
import org.sf.xrime.model.vertex.LabeledAdjVertex;


public class EgoCentricMergerReducer extends MapReduceBase 
    implements Reducer<Text, ObjectWritable, Text, LabeledAdjVertex> {

	@Override
	public void reduce(Text key, Iterator<ObjectWritable> values,
			OutputCollector<Text, LabeledAdjVertex> output, Reporter reporter)
			throws IOException {
		EgoCentricLabel label=new EgoCentricLabel();
		LabeledAdjVertex vertex=null;
		
		while (values.hasNext()) {
			ObjectWritable obj=values.next();
			
			if(obj.get() instanceof Edge) {
				label.addEdge((Edge) obj.get());
				continue;
			}
			
			if(obj.get() instanceof AdjVertex) {
				vertex=new LabeledAdjVertex((AdjVertex) obj.get());
			}
		}
		
		if(vertex!=null) {
			vertex.setLabel(EgoCentricAlgorithm.egoCentricLabelKey, label);
			System.out.println("emit: "+vertex);
			output.collect(key, vertex);
		}
	}
}
