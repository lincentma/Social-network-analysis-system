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
package org.sf.xrime.algorithms.pagerank.normal;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.ToolRunner;
import org.sf.xrime.ProcessorExecutionException;
import org.sf.xrime.algorithms.pagerank.PageRankLabel;
import org.sf.xrime.algorithms.transform.vertex.Vertex2LabeledTransformer;
import org.sf.xrime.model.Element;
import org.sf.xrime.model.label.LabelAdder;
import org.sf.xrime.model.label.Labelable;
import org.sf.xrime.model.vertex.LabeledAdjSetVertex;


/**
 * Transformer to add label need by pagerank to LabeledAdjSetVertex
 * AddPageRankLabelTransformer extends from Vertex2LabeledTransformer only to custom 
 * LabelAdder and output class LabeledAdjSetVertex in method execute().
 * @author Cai Bin
 */
public class AddPageRankLabelTransformer extends Vertex2LabeledTransformer {
  static final public String pageRankInitValueKey = "xrime.algorithm.pageRank.initValue";

  private double initValue=0;

  /**
   * Default constructor.
   */
  public AddPageRankLabelTransformer(){
    super();
  }

  /**
   * Normal constructor.
   * @param src date source dir
   * @param dest date destination dir
   */
  public AddPageRankLabelTransformer(Path src, Path dest) {
    super(src, dest);
  }

  /**
   * Get the initial value for nodes.
   * @return initial value.
   */
  public double getInitValue() {
    return initValue;
  }

  /**
   * Set the initial value for nodes.
   * @param initValue initial value.
   */
  public void setInitValue(double initValue) {
    this.initValue = initValue;
  }
  
  @Override
  public void setArguments(String[] params) throws ProcessorExecutionException {
    long number=0;
    List<String> other_args = new ArrayList<String>();
    for(int i=0; i < params.length; ++i) {
      try {
        if ("-i".equals(params[i])) {
          number=Long.parseLong(params[++i]);
          setInitValue(1/(double)number);
        } else {
          other_args.add(params[i]);
        }
      } catch (NumberFormatException except) {
        throw new ProcessorExecutionException(except);
      } catch (ArrayIndexOutOfBoundsException except) {
        throw new ProcessorExecutionException(except);
      }
    }
    // Make sure there are exactly 2 parameters left.
    if (other_args.size() != 2) {
      throw new ProcessorExecutionException("Wrong number of parameters: " +
          other_args.size() + " instead of 2.");
    }
    if(number<=0) {
      throw new ProcessorExecutionException("ERROR: -i <vertexNumber> should be great than 0");
    }
    
    setSrcPath(new Path(other_args.get(0)));
    setDestPath(new Path(other_args.get(1)));
  }

  public void execute() throws ProcessorExecutionException {
    setLabelAdderClass(PageRankLabelAdder.class);
    setOutputValueClass(LabeledAdjSetVertex.class);
    conf.set(pageRankInitValueKey, Double.toString(initValue));
    super.execute();
  }

  /**
   * Label Adder for pagerank algorithm.
   * @author Cai Bin
   */
  public static class PageRankLabelAdder extends LabelAdder {
    private double initValue=0;

    @Override
    public void addLabels(Labelable labels, Element element) {
      PageRankLabel label=(PageRankLabel) labels.getLabel(PageRankLabel.pageRankLabelKey);

      if(label==null) {
        label=new PageRankLabel(initValue);
        label.setInitWeight(initValue);
        label.setInitVertex(false);
        label.setReachable(true);
        label.setPrepPR(0);
      }

      labels.setLabel(PageRankLabel.pageRankLabelKey, label);
    }    

      /**
       * To get the initial value.
       * @see org.sf.xrime.model.label.LabelAdder#configure(org.apache.hadoop.mapred.JobConf)
       */
      public void configure(JobConf job)
      {
        super.configure(job);
        
      String property=job.get(pageRankInitValueKey);
      if(property!=null) {
        initValue=Double.valueOf(property);
      }
      }
  }

  /**
   * Main method for this transformer.
   * @param args input arguments
   */
  public static void main(String[] args) {
    try {
      int res = ToolRunner.run(new AddPageRankLabelTransformer(), args);
      System.exit(res);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
