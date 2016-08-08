/*
 * Copyright (C) quna@BUPT. 2009.
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
package org.sf.xrime.algorithms.HITS;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.util.ToolRunner;
import org.sf.xrime.ProcessorExecutionException;
import org.sf.xrime.algorithms.HITS.HITSLabel.AuthorityLabel;
import org.sf.xrime.algorithms.HITS.HITSLabel.HubLabel;
import org.sf.xrime.algorithms.statistics.VertexEdgeDoubleCounter;
import org.sf.xrime.model.vertex.LabeledAdjBiSetVertex;
import org.sf.xrime.model.vertex.Vertex;


/**
 * This class will compute the sum score of all vertex, which is the second step
 * of hits algorithm.
 * 
 * @author Qu Na
 * 
 */
public class HITSSummer extends VertexEdgeDoubleCounter {
  static final public String hubCounterKey = "hub.counter";
  static final public String authorityCounterKey = "authority.counter";

  public double getHubSum() {
    return getCounter(hubCounterKey);
  }

  public double getAuthoritySum() {
    return getCounter(authorityCounterKey);
  }

  @Override
  public void execute() throws ProcessorExecutionException {
    setDoubleCounterFilterClass(HITSSummerFilter.class);
    super.execute();
  }

  static public class HITSSummerFilter extends DoubleCounterFilter {
    Text hubKey = new Text(hubCounterKey);
    Text authorityKey = new Text(authorityCounterKey);
    DoubleWritable outputValue = new DoubleWritable();

    public HITSSummerFilter() {
    }

    @Override
    public void emit(Vertex value,
        OutputCollector<Text, DoubleWritable> output)
        throws IOException {
      LabeledAdjBiSetVertex vertex = (LabeledAdjBiSetVertex) value;

      if (vertex != null) {
        HubLabel hublabel = (HubLabel) vertex
            .getLabel(HITSAlgorithm.HITSHubKey);
        outputValue.set(hublabel.getHubscore());
        output.collect(hubKey, outputValue);

        AuthorityLabel authoritylabel = (AuthorityLabel) vertex
            .getLabel(HITSAlgorithm.HITSAuthorityKey);
        outputValue.set(authoritylabel.getAuthorityscore());
        output.collect(authorityKey, outputValue);
      }
    }
  }

  public static void main(String[] args) {
    HITSSummer algorithm = new HITSSummer();
    try {
      int res = ToolRunner.run(algorithm, args);
      System.out.println("Hub Sum:" + algorithm.getHubSum());
      System.out.println("Authority Sum:" + algorithm.getAuthoritySum());
      System.exit(res);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
