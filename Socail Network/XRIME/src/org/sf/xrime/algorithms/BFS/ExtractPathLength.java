package org.sf.xrime.algorithms.BFS;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.sf.xrime.ProcessorExecutionException;
import org.sf.xrime.algorithms.GraphAlgorithm;
import org.sf.xrime.algorithms.utils.GraphAlgorithmMapReduceBase;
import org.sf.xrime.model.vertex.LabeledAdjVertex;

/**
 * This class is used to determine the path length from specified starting vertex to each vertex in the graph.
 * @author weixue@cn.ibm.com
 *
 */
public class ExtractPathLength extends GraphAlgorithm {
	/**
	 * Determine path length from the path itself.
	 * @author weixue@cn.ibm.com
	 */
	public static class MapClass extends GraphAlgorithmMapReduceBase implements
		Mapper<Text, LabeledAdjVertex, Text, Text>{

		@Override
    public void map(Text key, LabeledAdjVertex value,
        OutputCollector<Text, Text> output, Reporter reporter)
        throws IOException {
			// Get the bfs label.
			BFSLabel label = (BFSLabel) value.getLabel(BFSLabel.bfsLabelPathsKey);
			if(label.getPreps()==null || label.getPreps().size()==0){
				output.collect(key, new Text("inf"));
			}else{
				output.collect(key, new Text(""+(label.getPreps().size()-1)));
			}
    }
	}

	@Override
	public void execute() throws ProcessorExecutionException {
    try {
      JobConf jobConf = new JobConf(context, ExtractPathLength.class);
      jobConf.setJobName("ExtractPathLength");
  
      jobConf.setMapperClass(MapClass.class);
      jobConf.setNumMapTasks(getMapperNum());        
      jobConf.setNumReduceTasks(0);
      jobConf.setOutputKeyClass(Text.class);
      jobConf.setOutputValueClass(Text.class);
          
      jobConf.setInputFormat(SequenceFileInputFormat.class);  
      jobConf.setOutputFormat(TextOutputFormat.class);    
      
      for(Path path : getSource().getPaths()){
        FileInputFormat.addInputPath(jobConf, path);
      }
      FileOutputFormat.setOutputPath(jobConf, context.getDestination().getPath());
  
      this.runningJob = JobClient.runJob(jobConf);
    } catch (IOException e) {
      throw new ProcessorExecutionException(e);
    } catch (IllegalAccessException e) {
      throw new ProcessorExecutionException(e);
    }	}
}
