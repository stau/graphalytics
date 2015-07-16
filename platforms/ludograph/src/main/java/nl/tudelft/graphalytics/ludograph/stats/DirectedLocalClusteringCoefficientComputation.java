/**
 * Copyright 2015 Delft University of Technology
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package nl.tudelft.graphalytics.ludograph.stats;

import com.google.common.collect.Iterables;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.tudelft.ludograph.graph.graph.vertices.NewDirectedVertex;
import org.tudelft.ludograph.graph.graph.vertices.mock.Edge;

import java.util.Properties;
import java.util.Set;

/**
 * DirectedLocalClusteringCoefficientComputation based on Tim Hegeman's implementation
 *
 * @author Sietse T. Au
 */
public class DirectedLocalClusteringCoefficientComputation extends
		NewDirectedVertex<LongWritable, DoubleWritable, NullWritable, LocalClusteringCoefficientMessage> {

	private Set<Long> collectNeighbourSet() {
		LongOpenHashSet set = new LongOpenHashSet();
		for (LongWritable neighbour : this.getNeighbors()) {
			set.add(neighbour.get());
		}
		return set;
	}
	
	private void sendConnectionInquiries(long sourceVertexId, Set<Long> neighbours) {
		// No messages to be sent if there is at most one neighbour
		if (neighbours.size() <= 1)
			return;
		
		// Send out inquiries in an all-pair fashion
		LongWritable messageDestinationId;
		for (long destinationNeighbour : neighbours) {
			LocalClusteringCoefficientMessage msg = new LocalClusteringCoefficientMessage(sourceVertexId, destinationNeighbour);
			for (long inquiredNeighbour : neighbours) {
				// Do not ask if a node is connected to itself
				if (destinationNeighbour == inquiredNeighbour)
					continue;
				messageDestinationId = new LongWritable(inquiredNeighbour); // need to construct new object, due to Map implementation.
				sendMsg(messageDestinationId, msg);
			}
		}
	}
	
	private void sendConnectionReplies(
			Iterable<LocalClusteringCoefficientMessage> inquiries) {
		// Construct a lookup set for the list of edges
		Set<Long> edgeLookup = new LongOpenHashSet();
		for (Edge<LongWritable, NullWritable> neighbour : getOutgoing())
			edgeLookup.add(neighbour.getI().get());
		// Loop through the inquiries and reply to those for which an edge exists
		LongWritable destinationId;
		LocalClusteringCoefficientMessage confirmation = new LocalClusteringCoefficientMessage();
		for (LocalClusteringCoefficientMessage msg : inquiries) {
			if (edgeLookup.contains(msg.getDestination())) {
				destinationId = new LongWritable(msg.getSource());
				sendMsg(destinationId, confirmation);
			}
		}
	}
	
	private static double computeLCC(double numberOfNeighbours, Iterable<LocalClusteringCoefficientMessage> messages) {
		// Any vertex with less than two neighbours can have no edges between neighbours; LCC = 0
		if (numberOfNeighbours < 2)
			return 0.0;

		// Count the number of (positive) replies
		long numberOfMessages = Iterables.size(messages);
		// Compute the LCC as the ratio between the number of existing edges and number of possible edges
		return numberOfMessages / numberOfNeighbours / (numberOfNeighbours - 1);
	}

	@Override
	public void compute(Iterable<LocalClusteringCoefficientMessage> messages) {
		if (getSuperStep() == 1) {
			// Second superstep: create a set of neighbours, for each pair ask if they are connected
			Set<Long> neighbours = collectNeighbourSet();
			sendConnectionInquiries(getId().get(), neighbours);
			setValue(new DoubleWritable(neighbours.size()));
			return;
		} else if (getSuperStep() == 2) {
			// Third superstep: for each inquiry reply iff the requested edge exists
			sendConnectionReplies(messages);
			return;
		} else if (getSuperStep() == 3) {
			// Fourth superstep: compute the ratio of responses to requests
			double lcc = computeLCC(getValue().get(), messages);
			getValue().set(lcc);
			// no aggregation
			aggregate(LocalClusteringCoefficientConfiguration.LCC_AGGREGATOR_NAME, new DoubleAverage(lcc));
			setHalt(true);
		}
	}

	@Override
	public boolean programProperties(Properties properties) {
		return false;
	}
}
