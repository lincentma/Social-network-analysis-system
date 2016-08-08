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
package org.sf.xrime.preprocessing.pajek;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.sf.xrime.ProcessorExecutionException;
import org.sf.xrime.Transformer;
import org.sf.xrime.model.edge.Edge;
import org.sf.xrime.model.vertex.AdjVertex;
import org.sf.xrime.preprocessing.smth.SmthReducer;


/**
 * @author Yang Cheng
 * 
 */
public class Pajek2AdjVertexTransformer extends Transformer {
	/**
	 * Default constructor.
	 */
	public Pajek2AdjVertexTransformer() {
		super();
	}

	/**
	 * Normal constructor.
	 * @param src
	 * @param dest
	 */
	public Pajek2AdjVertexTransformer(Path src, Path dest) {
		super(src, dest);
	}

	public static class MapClass extends MapReduceBase implements Mapper<LongWritable, Text, Text, AdjVertex> {
		Text vertex = new Text();
		AdjVertex adj=new AdjVertex();
		
		public void map(LongWritable key, Text value, 
				OutputCollector<Text, AdjVertex> output, 
				Reporter reporter) throws IOException {			
			adj.clearEdges();
			
			String line = value.toString().trim();
			String[] input=line.split(" ");
			String kind=input[0]; //get the kind of input, vertex, arch, or edge,indicated by the first letter
			String source,dest;

			if(kind.equals("v"))
			{
				source=input[1];
				vertex.set(source);
				output.collect(vertex, adj);  //emit the adj with no edge
				return;
			}

			if(kind.equals("a"))
			{
				source=input[1];
				dest=input[2];
				Edge edge=new Edge();
				edge.setFrom(source);
				edge.setTo(dest);
				adj.addEdge(edge);
				vertex.set(source);
				output.collect(vertex, adj);  // emit the adj with an edge with the source and dest
				return;
			}

			if(kind.equals("e"))              // emit two edges and each vertex as source and dest for once
			{
				source=input[1];
				dest=input[2];
				Edge edge=new Edge();
				edge.setFrom(source);
				edge.setTo(dest);
				adj.addEdge(edge);
				vertex.set(source);
				output.collect(vertex, adj); 
				
				adj.clearEdges();
				source=input[1];
				dest=input[2];
				Edge edge2=new Edge();
				edge2.setFrom(source);
				edge2.setTo(dest);
				adj.addEdge(edge2);
				vertex.set(source);
				output.collect(vertex, adj);
			}
		}
	}

	/**
	 * Merge the sublists of incoming adjacent vertexes of a vertex into one.
	 */
	/*
	public static class ReduceClass extends MapReduceBase implements Reducer<Text, AdjVertex, Text, AdjVertex> {

		public void reduce(Text key, Iterator<AdjVertex> values,
				OutputCollector<Text, AdjVertex> output, 
				Reporter reporter) throws IOException {

			HashSet<String> dest = new HashSet<String>();


			while (values.hasNext()) {
				AdjVertex neighbour = values.next();
				List<Edge> edges = neighbour.getEdges(); 
				if(edges!=null){
					for(Iterator<Edge> iterator = edges.iterator(); iterator.hasNext();){
						dest.add(iterator.next().getTo());                                   //get the dest of edge
					}
				}
			}

			// Create the AdjVertex instance.
			AdjVertex adj = new AdjVertex();
			String source = key.toString();
			// Set the vertex.
			adj.setId(source);


			Iterator<String> iterator;
			for(iterator = dest.iterator(); iterator.hasNext();){
				String destvert= iterator.next();
				Edge edge = new Edge();
				edge.setFrom(source);
				edge.setTo(destvert);
				adj.addEdge(edge);
			}
			output.collect(key, adj);
		}
	}*/

	public void execute() throws ProcessorExecutionException {
		JobConf jobConf =  new JobConf(conf, Pajek2AdjVertexTransformer.class);
		jobConf.setJobName("tansfer_pajek2Adjvert");

		jobConf.setMapperClass(MapClass.class);    
		jobConf.setReducerClass(SmthReducer.class);

		// makes the file format suitable for machine processing.
		jobConf.setOutputFormat(SequenceFileOutputFormat.class);

		// the keys are author names (strings)
		jobConf.setOutputKeyClass(Text.class);
		// the values are adjacent vertexes (Writable)
		jobConf.setOutputValueClass(AdjVertex.class);

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

	public static void main(String[] args){
	  try {
      int res = ToolRunner.run(new Pajek2AdjVertexTransformer(), args);
      System.exit(res);
    } catch (Exception e) {
      e.printStackTrace();
    }
	}
}
