package org.apache.giraph.examples;

import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.conf.LongConfOption;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.log4j.Logger;

import java.io.IOException;

@Algorithm(
    name = "Local Clustering Coefficient",
    description = "Find the local clustering coefficient for a node"
)
public class SimpleShortestPathsComputation extends BasicComputation<
    LongWritable, DoubleWritable, FloatWritable, DoubleWritable> {
  /** The node id */
  public static final LongConfOption SOURCE_ID =
      new LongConfOption("SimpleShortestPathsVertex.sourceId", 1,
          "The shortest paths id");
  /** Class logger */
  private static final Logger LOG =
      Logger.getLogger(LocalClusteringCoefficient.class);

  /**
   * Is this vertex the source id?
   *
   * @param vertex Vertex
   * @return True if the source id
   */
  private boolean isSource(Vertex<LongWritable, ?, ?> vertex) {
    return vertex.getId().get() == SOURCE_ID.get(getConf());
  }

  @Override
  public void compute(
      Vertex<LongWritable, ArrayWritable<LongWritable>, FloatWritable> vertex,
      Iterable<ArrayWritable<LongWritable>> messages) throws IOException {
    if (getSuperstep() == 0) {
	for (Edge<LongWritable, FloatWritable> edge : vertex.getEdges()) {
      		sendMessage(edge.getTargetVertexId(), vertex.getEdges());
	}
    }
    if (getSuperstep() == 1) {
	int sum = 0;
	for (Edge<LongWritable, FloatWritable> edge : vertex.getEdges()) {
      		LongWritable id = edge.getSourceVertexId();
		for (LongWritable v : messages) {
			if (v.get() == id.get()) {
				sum++;
			}
		}
	}
    }
    if (getSuperstep() == 2) {
	vertex.setValue(sum);
	voteToHalt();
    }
  }
}
