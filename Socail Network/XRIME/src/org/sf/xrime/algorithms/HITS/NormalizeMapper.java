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

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.sf.xrime.algorithms.HITS.HITSAlgorithm;
import org.sf.xrime.algorithms.HITS.HITSLabel.AuthorityLabel;
import org.sf.xrime.algorithms.HITS.HITSLabel.HubLabel;
import org.sf.xrime.algorithms.utils.GraphAlgorithmMapReduceBase;
import org.sf.xrime.model.vertex.LabeledAdjBiSetVertex;


/**
* Mapper of Normalize step, which is the third step of hits algorithm.
* This mapper will normalize the hub and authority score of each vertex.
* Compute that if the score of each vertex has converged. 
* @author Qu Na
* 
*/

public class NormalizeMapper extends GraphAlgorithmMapReduceBase 
  implements Mapper<Text, LabeledAdjBiSetVertex, Text, LabeledAdjBiSetVertex>{
  JobConf job = null;
  boolean changeFlag = false;
  private String continueFile;
  private double stopThreshold = 1/202024;
  double hubsum;
  double authoritysum;

  @Override
  public void map(Text key, LabeledAdjBiSetVertex value, 
      OutputCollector<Text, LabeledAdjBiSetVertex> collector, Reporter reporter)
      throws IOException {
    //nomalize the hub score
    HubLabel hublabel = (HubLabel) value.getLabel(HITSAlgorithm.HITSHubKey);
  
    hublabel.setHubscore(hublabel.getHubscore()/hubsum);
    value.setLabel(HITSAlgorithm.HITSHubKey, hublabel);
    
    //nomalize the authority score
    AuthorityLabel authoritylabel = (AuthorityLabel) value.getLabel(HITSAlgorithm.HITSAuthorityKey);
    
    authoritylabel.setAuthorityscore(authoritylabel.getAuthorityscore()/authoritysum);
    value.setLabel(HITSAlgorithm.HITSAuthorityKey, authoritylabel);
    
    double hubMSE = Math.abs(hublabel.getHubscore()-hublabel.getPreHubscore());
    double authorityMSE = Math.abs(authoritylabel.getAuthorityscore()-authoritylabel.getPreAuthorityscore());  
    
    if((hubMSE+authorityMSE) > stopThreshold){
      recordContinue();
    }
    
    //emit the vertex
    collector.collect(key, value);
  }
  
  public void configure(JobConf job){
    super.configure(job);
    
    this.job = job;
    String property=context.getParameter(HITSAlgorithm.HITSStopThresholdKey);
    if(property!=null) {
      stopThreshold=Double.valueOf(property);
    }
    
    property = context.getParameter(HITSSummer.hubCounterKey);
    if(property!=null){
      hubsum = Double.valueOf(property);
    }
    
    property = context.getParameter(HITSSummer.authorityCounterKey);
    if(property!=null){
      authoritysum = Double.valueOf(property);
    }
    
    continueFile=context.getParameter(NormalizeStep.continueFileKey);
    if(continueFile==null) {
      continueFile="continue";
    }
  }
  
  private void recordContinue() throws IOException {
    if(changeFlag) {
      return;
    }
    
    changeFlag=true;
    String continueFile=context.getParameter(NormalizeStep.continueFileKey);
          
    if(continueFile!=null) {
      FileSystem fs=FileSystem.get(job);
      fs.mkdirs(new Path(continueFile));
      //fs.close();
    }
  }
}
