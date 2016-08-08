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

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.sf.xrime.algorithms.utils.GraphAlgorithmMapReduceBase;
import org.sf.xrime.model.edge.AdjVertexEdge;
import org.sf.xrime.model.vertex.AdjSetVertex;
import org.sf.xrime.model.vertex.AdjVertex;


public class EgoCentricMapper extends GraphAlgorithmMapReduceBase 
    implements Mapper<Text, AdjVertex, Text, AdjSetVertex> {
	
	Text outputKey=new Text();
	AdjSetVertex outputValue=new AdjSetVertex();
	
	@Override
	public void map(Text key, AdjVertex adjVertex,
			OutputCollector<Text, AdjSetVertex> collector,
			Reporter reporter) throws IOException {
		outputValue.fromAdjVertexTos(adjVertex);
		 
		for(AdjVertexEdge dest: outputValue.getOpposites()) {
			outputKey.set(dest.getOpposite());
			collector.collect(outputKey, outputValue);
		}
	}
}
