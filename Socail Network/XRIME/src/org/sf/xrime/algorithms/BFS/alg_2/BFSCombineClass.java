package org.sf.xrime.algorithms.BFS.alg_2;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.sf.xrime.algorithms.utils.GraphAlgorithmMapReduceBase;
import org.sf.xrime.model.vertex.LabeledAdjVertex;


/**
 * Helper class to reduce the size of shuffle, which is important for some scenarios.
 * @author weixue@cn.ibm.com
 *
 */
public class BFSCombineClass extends GraphAlgorithmMapReduceBase implements
    Reducer<Text, LabeledAdjVertex, Text, LabeledAdjVertex> {

  @Override
  public void reduce(Text key, Iterator<LabeledAdjVertex> values,
      OutputCollector<Text, LabeledAdjVertex> output, Reporter reporter)
      throws IOException {
    boolean got_notifier = false;
    while (values.hasNext()) {
      LabeledAdjVertex vertex = values.next();
      
      if(vertex.getId().compareTo(key.toString())!=0) {
        if(!got_notifier){
          got_notifier = true;
          output.collect(key, vertex); // Only one notifier is needed.
        }
        continue;
      }
      
      // The vertex itself. There should be only one record of this kind.
      output.collect(key, vertex);
    }
  }
}
