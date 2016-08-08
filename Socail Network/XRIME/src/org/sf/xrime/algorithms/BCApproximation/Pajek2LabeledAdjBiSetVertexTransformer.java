/*
 * Copyright (C) yangcheng@BUPT. 2009.
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
package org.sf.xrime.algorithms.BCApproximation;


import java.io.IOException;

import java.util.Iterator;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;

import org.sf.xrime.ProcessorExecutionException;
import org.sf.xrime.Transformer;
import org.sf.xrime.model.vertex.LabeledAdjBiSetVertex;
import org.sf.xrime.model.edge.AdjVertexEdge;

/**
 * @author Yang Cheng
 * the function is to transform  the input format from Pajek to the format we need in 
 * BC Algorithm
 * the input's format: there exists two kinds of input
 * V: indicates this is a vertex
 * e: indicates this is an undirected edge
 * 
 * the output's format:
 * (key, value)
 * key is the Text
 * value is the LabeledAdjBiSetVertex whose id is key
 *  
 * 
 * 
 */
public class Pajek2LabeledAdjBiSetVertexTransformer extends Transformer {
  /**
   * Default constructor.
   */
  String src, dest;
  public Pajek2LabeledAdjBiSetVertexTransformer() {
    super();
//    System.out.println("00000000000000000000000000000000000000000000000000000000000000000");
  }

  /**
   * Normal constructor.
   * @param src
   * @param dest
   */
  public Pajek2LabeledAdjBiSetVertexTransformer(String src,String dest)
  {
    this.src=src;
    this.dest=dest;
  }
  
  public Pajek2LabeledAdjBiSetVertexTransformer(Path src, Path dest) {
    super(src, dest);
  }
  public String getSrc()
  {
    return src;
  }
  public String getDest()
  {
    return dest;
  }

  public static class MapClass extends MapReduceBase implements Mapper<LongWritable, Text, Text,Text> {
    
    
    Text vertsource=new Text();
    Text vertdest=new Text();
    
    public void map(LongWritable key, Text value, 
      OutputCollector<Text, Text> output, 
      Reporter reporter) throws IOException {    
      String line = value.toString().trim();
      String[] input=line.split(" ");
      String kind=input[0]; //get the kind of input, vertex, arch, or edge,indicated by the first letter
      String source,dest;
      if(kind.equals("v"))             // just simple vertex
      {
        source=input[1];
        vertsource.set(source);
        vertdest.set("dest");
        output.collect(vertsource, vertdest);
        
      }
      
      if(kind.equals("e"))                            //an  undirected edge
      {
        source=input[1];
        dest=input[2];
        vertsource.set(source);
        vertdest.set(dest);
        output.collect(vertsource, vertdest);
        output.collect(vertdest, vertsource);
        
      }
      
  }
  }
  
  
  public static class ReduceClass extends MapReduceBase implements Reducer<Text, Text, Text, LabeledAdjBiSetVertex> {
    LabeledAdjBiSetVertex vertex=new LabeledAdjBiSetVertex();
    
    public void reduce(Text key, Iterator<Text> values,
        OutputCollector<Text, LabeledAdjBiSetVertex> output, 
        Reporter reporter) throws IOException {
          vertex.setId(key.toString());
          AdjVertexEdge edge=new AdjVertexEdge();
          LabeledAdjBiSetVertex vertex=new LabeledAdjBiSetVertex();
          vertex.setId(key.toString());
//          System.out.println("correct so far------------+11111111111111"+vertex);
          String opposite;
          while(values.hasNext())
          {  
            opposite=values.next().toString();
            if(opposite.equals("dest"))                        //means the input is a vertex and ignore this information
            {
              continue;
            }
            edge.setOpposite(opposite);
//            System.out.println("opposite of "+key.toString()+"is"+opposite+"-------");
//            System.out.println("the edge is"+edge);
            
            vertex.addBackwardVertex((AdjVertexEdge)edge.clone());
            vertex.addForwardVertex((AdjVertexEdge)edge.clone());
//              System.out.println("correct so far------------+222222222222"+vertex);
            }
          BCLabel label=(BCLabel)vertex.getLabel(BCLabel.bcLabelPathsKey);
          if(label==null)
          {
            label=new BCLabel();
            label.setBC(0);
            label.setDistance(-1);
            label.setNumber(0);
            vertex.setLabel(BCLabel.bcLabelPathsKey, label);
          }
//          System.out.println(label+"----------------------------label");
//          System.out.println(vertex+" ----------------------------vertex");
          output.collect(key, vertex);
      
          }
  }
  
  public void execute() throws ProcessorExecutionException {
    JobConf jobConf =  new JobConf(conf, Pajek2LabeledAdjBiSetVertexTransformer.class);
    jobConf.setJobName("tansfer_pajek2LabeledAdjBiSetvert");

    jobConf.setMapperClass(MapClass.class);    
    jobConf.setReducerClass(ReduceClass.class);

    
    jobConf.setOutputFormat(SequenceFileOutputFormat.class);

    jobConf.setMapOutputKeyClass(Text.class);
    jobConf.setMapOutputValueClass(Text.class);
    jobConf.setOutputKeyClass(Text.class);
    
    jobConf.setOutputValueClass(LabeledAdjBiSetVertex.class);

    FileInputFormat.setInputPaths(jobConf, srcPath);
    FileOutputFormat.setOutputPath(jobConf, destPath);

    jobConf.setNumMapTasks(mapperNum);
    jobConf.setNumReduceTasks(reducerNum);

    try {
      this.runningJob = JobClient.runJob(jobConf);
    } catch (IOException e) {
      throw new ProcessorExecutionException(e);
    }
  }
}
    