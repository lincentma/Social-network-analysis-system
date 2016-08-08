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
package org.sf.xrime.algorithms.BC;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.GenericOptionsParser;
import java.util.Set;
import java.util.HashSet;

/*
 * @author Yang Cheng
 * 
 * This is the class which runs the BC algorithms 
 * the program has four parameters:
 * src: means the text source to get the vertexes from the initial input of the whole program
 * mediate: means the binary source of the program, the output of the Pajek2LabeledAdjBiSetVertexTransformer ,the input of BC algorithms,it includes the information of the whole graph
 * out: means the program's out which is the input of the intermediate result
 * v: the specified vertex
 * the BC of the specified vertex is printed in the console 
 * 
 */

public class BCRunner {  
  private static float bc=0;
  
  public static void main(String[] args) {
    runBC(args);
  }
    
  public static void runBC(String[] args) {
    try {            
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 4) {
            System.err.println("Usage:  <in> <mediate> <out> <vertex>");
            System.exit(1);
        }
       
        Set<String> vertexes=new HashSet<String>();
        String src=otherArgs[0];         //src file
        String mediate=otherArgs[1];                //mediate result
        String out=otherArgs[2];         //out
        String v=otherArgs[3];                       //specified vertex
        
        BCAlgorithm job=new BCAlgorithm(v);
        Pajek2LabeledAdjBiSetVertexTransformer transformer = 
          new Pajek2LabeledAdjBiSetVertexTransformer(src,mediate);
        transformer.execute();
        
        job.setSrcPath(new Path(mediate));
        job.setDestPath(new Path(out));
        
        vertexes=new BCVertexGeter(src).getBCVertex();
        if(!vertexes.contains(v))
        {
          System.err.println("Wrong vertex");
          System.exit(1);
        }

        for(String item:vertexes)
        {
          job.addInitVertex(item);
          job.execute();
          bc+=job.getBCvertex();
          job.setBCvertex(0);
        }
        
        System.out.println("the bc of vertex   "+v+"   is   "+bc);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
