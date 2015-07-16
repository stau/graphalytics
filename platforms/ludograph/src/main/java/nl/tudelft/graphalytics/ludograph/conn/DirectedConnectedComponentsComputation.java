/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package nl.tudelft.graphalytics.ludograph.conn;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.tudelft.ludograph.graph.graph.vertices.NewDirectedVertex;

import java.util.Properties;

/**
 * Directed Connected Components implementation based on Wing Ngai and Tim Hegeman implementation.
 *
 * @author Sietse Au
 */
public class DirectedConnectedComponentsComputation extends NewDirectedVertex<LongWritable, LongWritable, NullWritable, LongWritable> {

	@Override
	public void compute(Iterable<LongWritable> messages) {
		long currentComponent = getValue().get();

		// directed vertex in ludograph will load incoming and outgoing edges
		// due to the vertex format.
		if (getSuperStep() == 1) {
			// Initialize value to own id
			setValue(getId());

			for (LongWritable neighbor : getNeighbors()) {
				if (neighbor.get() < currentComponent) {
					currentComponent = neighbor.get();
				}
			}
			// only need to send value if it is not the own id
			if (currentComponent != getValue().get()) {
				setValue(new LongWritable(currentComponent));
				for (LongWritable neighbor : getNeighbors()) {
					if (neighbor.get() > currentComponent) {
						sendMsg(neighbor, getValue());
					}
				}
			}

			this.setHalt(true);
			return;
		}

		boolean changed = false;
		// did we get a smaller id ?
		for (LongWritable message : messages) {
			long candidateComponent = message.get();
			if (candidateComponent < currentComponent) {
				currentComponent = candidateComponent;
				changed = true;
			}
		}

		// propagate new component id to the neighbors
		if (changed) {
			setValue(new LongWritable(currentComponent));
			sendToAll(getValue());
		}
		setHalt(true);
	}

	@Override
	public boolean programProperties(Properties properties) {
		return true;
	}
}