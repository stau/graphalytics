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
package nl.tudelft.graphalytics.ludograph.cd;

import nl.tudelft.graphalytics.domain.algorithms.CommunityDetectionParameters;
import nl.tudelft.graphalytics.ludograph.ParameterConfiguration;
import nl.tudelft.graphalytics.ludograph.cd.format.CDRecordWriter;
import org.apache.hadoop.conf.Configuration;
import org.tudelft.ludograph.conf.LudographConfiguration;

/**
 * Configuration constants for community detection on Giraph, constant naming same as Giraph implementation
 * 
 * @author Sietse T. Au
 */
public final class CommunityDetectionConfiguration implements ParameterConfiguration {

	public static final String NODE_PREFERENCE_KEY = "graphalytics.cd.node-preference";
	public static final String HOP_ATTENUATION_KEY = "graphalytics.cd.hop-attenuation";
	public static final String MAX_ITERATIONS_KEY = "graphalytics.cd.max-iterations";

	public CommunityDetectionConfiguration() {
	}

	public Configuration toConfiguration(Object parameters) {
		CommunityDetectionParameters params = (CommunityDetectionParameters) parameters;
		Configuration configuration = new Configuration(false);
		configuration.setFloat(NODE_PREFERENCE_KEY, params.getNodePreference());
		configuration.setFloat(HOP_ATTENUATION_KEY, params.getHopAttenuation());
		configuration.setLong(MAX_ITERATIONS_KEY, params.getMaxIterations());
		configuration.set(LudographConfiguration.RECORD_WRITER_CLASS, CDRecordWriter.class.getName());
		return configuration;
	}

}
