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
import java.util.Iterator;

import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.sf.xrime.algorithms.HITS.DeliveryStep;
import org.sf.xrime.algorithms.HITS.HITSAlgorithm;
import org.sf.xrime.algorithms.HITS.HITSLabel.AuthorityLabel;
import org.sf.xrime.algorithms.HITS.HITSLabel.HubLabel;
import org.sf.xrime.algorithms.utils.GraphAlgorithmMapReduceBase;
import org.sf.xrime.model.vertex.LabeledAdjBiSetVertex;


/**
* Reducer of DeliveryStep, which is the first step of hits algorithm.
* This reducer will compute the sum of the score received from its neighbors.
* @author Qu Na
* 
*/

public class DeliveryReducer extends GraphAlgorithmMapReduceBase 
  implements Reducer<Text, ObjectWritable, Text, LabeledAdjBiSetVertex> {
  JobConf job = null;
  private String continueFile;
  
  @Override
  public void reduce(Text key, Iterator<ObjectWritable> values,
      OutputCollector<Text, LabeledAdjBiSetVertex> output, Reporter reporter)
      throws IOException{
    LabeledAdjBiSetVertex vertex = null;
//    double oldhubscore = 0;
    double newhubscore = 0;
//    double oldauthorityscore = 0;
    double newauthorityscore = 0;
    
    while(values.hasNext()){
      ObjectWritable obj = values.next();
      
      if(obj.get() instanceof HubLabel){
        newauthorityscore += ((HubLabel)obj.get()).getHubscore();
        continue;
      }else if(obj.get() instanceof AuthorityLabel){
        newhubscore += ((AuthorityLabel)obj.get()).getAuthorityscore();
        continue;
      }
      vertex = (LabeledAdjBiSetVertex) obj.get();
    }
    
    if(vertex!=null){
      HubLabel hublabel = (HubLabel) vertex.getLabel(HITSAlgorithm.HITSHubKey);
      AuthorityLabel authoritylabel = (AuthorityLabel) vertex.getLabel(HITSAlgorithm.HITSAuthorityKey);
            
      hublabel.setHubscore(newhubscore);
      vertex.setLabel(HITSAlgorithm.HITSHubKey,hublabel);
      
      authoritylabel.setAuthorityscore(newauthorityscore);
      vertex.setLabel(HITSAlgorithm.HITSAuthorityKey,authoritylabel);
      
      output.collect(key,vertex);
    }
  }
  
  public void configure(JobConf job){
    super.configure(job);
    this.job = job;
    continueFile=context.getParameter(DeliveryStep.continueFileKey);
    if(continueFile==null) {
      continueFile="continue";
    }
  }
}
