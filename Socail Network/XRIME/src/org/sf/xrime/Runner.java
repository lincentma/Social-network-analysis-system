package org.sf.xrime;

import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.sf.xrime.algorithms.BFS.BFSLabelTransformer;
import org.sf.xrime.algorithms.BFS.alg_1.BFSAlgorithm;
import org.sf.xrime.algorithms.HITS.HITSAlgorithm;
import org.sf.xrime.algorithms.MST.MSTAlgorithm;
import org.sf.xrime.algorithms.clique.maximal.MaximalStrongCliqueAlgorithm;
import org.sf.xrime.algorithms.clique.maximal.MaximalWeakCliqueAlgorithm;
import org.sf.xrime.algorithms.kcore.undirected.SpecifiedKCoreAlgorithm;
import org.sf.xrime.algorithms.layout.circular.CircularLayoutAlgorithm;
import org.sf.xrime.algorithms.layout.ellipse.EllipseLayoutAlgorithm;
import org.sf.xrime.algorithms.layout.gfr.GridVariantFRAlgorithm;
import org.sf.xrime.algorithms.layout.radialtree.RadialTreeAlgorithm;
import org.sf.xrime.algorithms.pagerank.normal.PageRankRunner;
import org.sf.xrime.algorithms.partitions.VertexDegree;
import org.sf.xrime.algorithms.partitions.connected.bi.BCCAlgorithm;
import org.sf.xrime.algorithms.partitions.connected.strongly.SCCAlgorithm;
import org.sf.xrime.algorithms.partitions.connected.weakly.alg_1.SetBasedAlgorithm;
import org.sf.xrime.algorithms.partitions.connected.weakly.alg_2.TraversalBasedAlgorithm;
import org.sf.xrime.algorithms.statistics.AverageVertexDegree;
import org.sf.xrime.algorithms.statistics.LargestVertexDegree;
import org.sf.xrime.algorithms.statistics.VertexEdgeCounter;
import org.sf.xrime.algorithms.statistics.VertexEdgeDoubleCounter;
import org.sf.xrime.algorithms.statistics.egoCentric.EgoCentricAlgorithm;
import org.sf.xrime.algorithms.transform.vertex.AdjVertex2AdjSetVertexTransformer;
import org.sf.xrime.algorithms.transform.vertex.InAdjVertex2AdjBiSetVertexTransformer;
import org.sf.xrime.algorithms.transform.vertex.OutAdjVertex2AdjBiSetVertexTransformer;
import org.sf.xrime.algorithms.transform.vertex.OutAdjVertex2AdjSetVertexTransformer;
import org.sf.xrime.algorithms.transform.vertex.Vertex2LabeledTransformer;
import org.sf.xrime.postprocessing.SequenceFileToTextFileTransformer;
import org.sf.xrime.preprocessing.smth.Raw2InAdjVertexTransformer;
import org.sf.xrime.preprocessing.smth.Raw2OutAdjVertexTransformer;
import org.sf.xrime.preprocessing.smth.Raw2SortedInAdjVertexTransformer;
import org.sf.xrime.preprocessing.smth.Raw2SortedOutAdjVertexTransformer;
import org.sf.xrime.preprocessing.smth.SmthTransformer;
import org.sf.xrime.preprocessing.textadj.TextAdjTransformer;

/**
 * Entry main class for X-RIME package's command line.
 * @author xue
 *
 */
public class Runner {
  /** All transformers of this Runner.*/
  private static HashMap<String, Class<? extends Tool>> transformers = 
    new HashMap<String, Class<? extends Tool>>();
  /** All algorithms of this Runner.*/
  private static HashMap<String, Class<? extends Tool>> algorithms =
    new HashMap<String, Class<? extends Tool>>();
  
  static{
    transformers.put("raw2inadj", Raw2InAdjVertexTransformer.class);
    transformers.put("raw2outadj", Raw2OutAdjVertexTransformer.class);
    transformers.put("raw2sinadj", Raw2SortedInAdjVertexTransformer.class);
    transformers.put("raw2soutadj", Raw2SortedOutAdjVertexTransformer.class);
    transformers.put("adj2adjset", AdjVertex2AdjSetVertexTransformer.class);
    transformers.put("2labeled", Vertex2LabeledTransformer.class);
    transformers.put("inadj2biset", InAdjVertex2AdjBiSetVertexTransformer.class);
    transformers.put("outadj2set", OutAdjVertex2AdjSetVertexTransformer.class);
    transformers.put("outadj2biset", OutAdjVertex2AdjBiSetVertexTransformer.class);
    transformers.put("smthtr", SmthTransformer.class);
    transformers.put("txt2adj", TextAdjTransformer.class);
    transformers.put("seq2txt", SequenceFileToTextFileTransformer.class);
    transformers.put("bfslabeltr", BFSLabelTransformer.class);
    
    algorithms.put("degree", VertexDegree.class);
    algorithms.put("avgdegree", AverageVertexDegree.class);
    algorithms.put("maxdegree", LargestVertexDegree.class);
    algorithms.put("edgecounter", VertexEdgeCounter.class);
    algorithms.put("edgecounter2", VertexEdgeDoubleCounter.class);
    algorithms.put("egocentric", EgoCentricAlgorithm.class);
    algorithms.put("wcc1", SetBasedAlgorithm.class);
    algorithms.put("wcc2", TraversalBasedAlgorithm.class);
    algorithms.put("scc", SCCAlgorithm.class);
    algorithms.put("bcc", BCCAlgorithm.class);
    algorithms.put("kcore", SpecifiedKCoreAlgorithm.class);
    algorithms.put("maxwc", MaximalWeakCliqueAlgorithm.class);
    algorithms.put("maxsc", MaximalStrongCliqueAlgorithm.class);
    algorithms.put("cirlayout", CircularLayoutAlgorithm.class);
    algorithms.put("elllayout", EllipseLayoutAlgorithm.class);
    algorithms.put("gfrlayout", GridVariantFRAlgorithm.class);
    algorithms.put("radlayout", RadialTreeAlgorithm.class);
    algorithms.put("pagerank", PageRankRunner.class);
    algorithms.put("hits", HITSAlgorithm.class);
    algorithms.put("mst", MSTAlgorithm.class);
    algorithms.put("bfs1", BFSAlgorithm.class);
    algorithms.put("bfs2", org.sf.xrime.algorithms.BFS.alg_2.BFSAlgorithm.class);
  }
  
  /**
   * Utility used to generate nice layout help message.
   * @param target_length
   * @param current_length
   * @return
   */
  private static String padding(int target_length, int current_length){
    String result = "";
    while(current_length<target_length){
      result+=" ";
      current_length++;
    }
    return result;
  }
  
  /**
   * Print the usage of this Runner.
   */
  private static void printHelp(){
    String help = "Usage: java org.sf.xrime.Runner {processor} {params} \n" +
                  "Supported transformers are: \n";
    for(Entry<String, Class<? extends Tool>> entry: transformers.entrySet()){
      String temp_str = "    " + entry.getKey();
      help +=  temp_str + padding(25, temp_str.length()) + entry.getValue().getCanonicalName() + "\n";;
    }
    help += "Supported algorithms are: \n";
    for(Entry<String, Class<? extends Tool>> entry: algorithms.entrySet()){
      String temp_str = "    " + entry.getKey();
      help += temp_str + padding(25, temp_str.length()) + entry.getValue().getCanonicalName() + "\n";;
    }
    System.out.println(help);
  }

  /**
   * @param args
   */
  public static void main(String[] args) {
    Tool processor = null;

    if (args.length < 1) {
      System.out.println("No processor specified.");
      printHelp();
      System.exit(-1);
    }

    try {
      if (!(transformers.containsKey(args[0]) || algorithms.containsKey(args[0]))) {
        Class<? extends Tool> processor_class = Class.forName(args[0]).asSubclass(Tool.class);
        if (processor_class != null) {
          processor = processor_class.newInstance();
        } else {
          System.out.println("Unknown processor.");
          printHelp();
          System.exit(-1);
        }
      } else {
        processor = transformers.containsKey(args[0]) ? transformers.get(
            args[0]).newInstance() : algorithms.get(args[0]).newInstance();
      }
      String[] params = new String[args.length - 1];
      System.arraycopy(args, 1, params, 0, args.length - 1);
      int res = ToolRunner.run(processor, params);
      System.exit(res);
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
      System.exit(-1);
    } catch (IllegalAccessException e) {
      e.printStackTrace();
      System.exit(-1);
    } catch (InstantiationException e) {
      e.printStackTrace();
      System.exit(-1);
    } catch (Exception e) {
      e.printStackTrace();
      System.exit(-1);
    }
  }
}
