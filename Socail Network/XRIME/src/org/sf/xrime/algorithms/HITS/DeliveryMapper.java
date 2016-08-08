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

import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.sf.xrime.algorithms.HITS.HITSAlgorithm;
import org.sf.xrime.algorithms.HITS.HITSLabel.AuthorityLabel;
import org.sf.xrime.algorithms.HITS.HITSLabel.HubLabel;
import org.sf.xrime.algorithms.utils.GraphAlgorithmMapReduceBase;
import org.sf.xrime.model.edge.AdjVertexEdge;
import org.sf.xrime.model.vertex.LabeledAdjBiSetVertex;


/**
* Mapper of DeliveryStep, which is the first step of hits algorithm.
* This mapper will deliver the hub score to each vertex's forward and deliver the authority score to each vertex's backward.
* @author Qu Na
* 
*/

public class DeliveryMapper extends GraphAlgorithmMapReduceBase 
  implements Mapper<Text, LabeledAdjBiSetVertex, Text, ObjectWritable> {
  JobConf jobConf = null;
  
  ObjectWritable outputValue = new ObjectWritable();
  Text hubdest = new Text();
  Text authoritydest = new Text();
  double prehubscore = 0;
  double preauthorityscore = 0;
  
  HubLabel hublabel = new HubLabel();
  AuthorityLabel authoritylabel = new AuthorityLabel();
    
  @Override
  public void map(Text key, LabeledAdjBiSetVertex value, 
      OutputCollector<Text, ObjectWritable> collector, Reporter reporter)
      throws IOException{
    HubLabel hublabel=(HubLabel) value.getLabel(HITSAlgorithm.HITSHubKey);
    if(hublabel==null) {
      hublabel=new HubLabel();
    }
    
    //update the previous hub score
    prehubscore = hublabel.getHubscore();
    hublabel.setPreHubscore(prehubscore);
    
    AuthorityLabel authoritylabel = (AuthorityLabel) value.getLabel(HITSAlgorithm.HITSAuthorityKey);
    if(authoritylabel == null){
      authoritylabel = new AuthorityLabel();
    }
    //if the vertex has no neighbors, set 0 to its score
    if(value.getBackwardVertexes().size()==0){
      authoritylabel.setAuthorityscore(0);
      authoritylabel.setPreAuthorityscore(0);
    }
    //update the previous authority score
    preauthorityscore=authoritylabel.getAuthorityscore();
    authoritylabel.setPreAuthorityscore(preauthorityscore);
    
    value.setLabel(HITSAlgorithm.HITSHubKey, hublabel);
    
    // emit hub for forwards
    if(value.getForwardVertexes().size()>0) {
      double hubscore=hublabel.getHubscore();
      
      hublabel.setHubscore(hubscore);
       outputValue.set(hublabel);
        
       for(AdjVertexEdge edge: value.getForwardVertexes()) {
          hubdest.set(edge.getOpposite());
         collector.collect(hubdest, outputValue);
        }
    } 
    if(value.getForwardVertexes().size()==0){
      hublabel.setHubscore(0);
      hublabel.setPreHubscore(0);
    }
    

    value.setLabel(HITSAlgorithm.HITSAuthorityKey, authoritylabel);
      
    // emit authority for backwards
    if(value.getBackwardVertexes().size()>0) {
      double authorityscore=authoritylabel.getAuthorityscore();
      
      authoritylabel.setAuthorityscore(authorityscore);
       outputValue.set(authoritylabel);
        
       for(AdjVertexEdge edge: value.getBackwardVertexes()) {
        authoritydest.set(edge.getOpposite());
        collector.collect(authoritydest, outputValue);
        }
    } 
    
    // emit the vertex
    outputValue.set(value);
    collector.collect(key, outputValue);
    
  }
}
