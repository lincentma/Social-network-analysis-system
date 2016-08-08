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
package org.sf.xrime.algorithms.pagerank.priors;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.util.ToolRunner;
import org.sf.xrime.ProcessorExecutionException;
import org.sf.xrime.algorithms.setBFS.SetBFSLabel;
import org.sf.xrime.algorithms.statistics.VertexEdgeCounter;
import org.sf.xrime.model.vertex.LabeledAdjSetVertex;
import org.sf.xrime.model.vertex.Vertex;


public class PageRankWithPriorsVertexCounter extends VertexEdgeCounter {
  public void execute() throws ProcessorExecutionException {
    setCounterFilterClass(PageRankWithPriorsVerterCounterFilter.class);
    super.execute();
  }
  
  static public class PageRankWithPriorsVerterCounterFilter extends CounterFilter {
    static final public String vertexCounterKey = "pr.priors.vertex.counter";
    static final public String initVertexCounterKey = "pr.priors.init.vertex.counter";
    
    LabeledAdjSetVertex labelVertex;
    Text key=new Text(vertexCounterKey);
    Text key2=new Text(initVertexCounterKey);
    LongWritable one=new LongWritable(1);
    
    public PageRankWithPriorsVerterCounterFilter(){      
    }

    @Override
    public void emit(Vertex value, OutputCollector<Text, LongWritable> output) throws IOException {
          if(value instanceof LabeledAdjSetVertex) {
            labelVertex=(LabeledAdjSetVertex) value;
            
            SetBFSLabel setBFSLabel=(SetBFSLabel) labelVertex.getLabel(SetBFSLabel.setBFSLabelPathsKey);
            
            if(setBFSLabel!=null && setBFSLabel.getStatus()==1) {
              output.collect(key, one);
              
              if(setBFSLabel.getDistance()==0) {
                output.collect(key2, one);
              }
            }
          }
    }
  }  

  public static void main(String[] args){
    try {
      int res = ToolRunner.run(new PageRankWithPriorsVertexCounter(), args);
      System.exit(res);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
