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

import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.tudelft.ludograph.graph.graph.vertices.NewDirectedVertex;
import org.tudelft.ludograph.graph.graph.vertices.mock.Edge;

import java.util.*;

/**
 * Community Detection algorithm
 * Credits: mostly Marcin's code refactored
 * Detect Community using algorithm by methods provided in
 * "Towards Real-Time Community Detection in Large Networks by Ian X.Y. Leung,Pan Hui,Pietro Li,and Jon Crowcroft"
 * Changes
 * - refactored private attributes to CommunityDetectionWritable
 * - refactored very long methods
 * - removed unused attributes.
 * Question
 * - why are there two iteration thresholds?
 *
 * Note: Value on edge is true iff the edge is bidirectional. These edges have double weight in the label selection
 * process.
 *
 * @author Wing Ngai
 * @author Tim Hegeman
 */
public class DirectedCommunityDetectionComputation extends NewDirectedVertex<LongWritable, CDLabel, NullWritable, Text> {
    // Load the parameters from the configuration before the compute method to save expensive lookups
	private static float nodePreference;
	private static float hopAttenuation;
	private static int maxIterations;

    private LongOpenHashSet bidirectionalEdges = new LongOpenHashSet();

    @Override
    public void compute(Iterable<Text> messages) {
        // max iteration, a stopping condition for data-sets which do not converge
        if (this.getSuperStep() > maxIterations+1) {
            setHalt(true);
            return;
        }
        // start community detection rounds
        if (this.getSuperStep() == 1) {

            // check bidirectionality
            LongOpenHashSet incoming = new LongOpenHashSet();
            for (Edge<LongWritable, NullWritable> edge : getIncoming()) {
                incoming.add(edge.getI().get());
            }

            for (Edge<LongWritable, NullWritable> edge : getOutgoing()) {
                if (incoming.contains(edge.getI().get())) {
                    bidirectionalEdges.add(edge.getI().get());
                }
            }
            incoming.clear();

            // initialize algorithm, set label as the vertex id, set label score as 1.0
            CDLabel cd = new CDLabel(String.valueOf(getId().get()), 1.0f);
            setValue(cd);

            // send initial label to all neighbors
            propagateLabel();
            return;
        }
        else {
            // label assign
            determineLabel(messages);
            propagateLabel();
        }
    }

    /**
     * Propagate label information to neighbors
     */
    private void propagateLabel() {
        CDLabel cd = getValue();
        for (LongWritable neighbour : getNeighbors()) {
            Text initMessage = new Text(getId().get() + "," + cd.getLabelName() + "," + cd.getLabelScore() + "," + neighboursSize());
            sendMsg(neighbour, initMessage);
        }
    }

    /**
     * Chooses new label AND updates label score
     * - chose new label based on SUM of Label_score(sum all scores of label X) x f(i')^m, where m is number of edges (ignore edge weight == 1) -> EQ 2
     * - score of a vertex new label is a maximal score from all existing scores for that particular label MINUS delta (specified as input parameter) -> EQ 3
     */
    private void determineLabel(Iterable<Text> messages) {

        CDLabel cd = getValue();
        String oldLabel = cd.getLabelName().toString();

        // fill in the labelAggScoreMap and labelMaxScoreMap from the received messages (by EQ2 step 1)
        Map<String, CDLabelStatistics> labelStatsMap = groupLabelStatistics(messages);

        // choose label based on the gathered label info (by EQ2 step 2)
        String chosenLabel = chooseLabel(labelStatsMap);
        cd.setLabelName(new Text(chosenLabel));

        // update new label score by EQ3
        float updatedLabelScore = getChosenLabelScore(labelStatsMap, chosenLabel, oldLabel);
        cd.setLabelScore(updatedLabelScore);
    }

    /**
     * Calculate the aggregated score and max score per distinct label. (EQ 2 step 1)
     */
    public Map<String, CDLabelStatistics> groupLabelStatistics(Iterable<Text> messages) {

        Map<String, CDLabelStatistics> labelStatsMap = new HashMap<String, CDLabelStatistics>();

        // group label statistics
	    LongWritable sourceId = new LongWritable();
        for (Text message : messages) {

            CDMessage cdMsg = CDMessage.FromText(message);
	        sourceId.set(cdMsg.getSourceId());
            String labelName = cdMsg.getLabelName();
            float labelScore = cdMsg.getLabelScore();
            int f = cdMsg.getF();

            float weightedLabelScore = labelScore * (float) Math.pow((double) f, (double) nodePreference);

	        // Double score if edge is bidirectional
	        if (bidirectionalEdges.contains(sourceId.get())) {
		        weightedLabelScore *= 2;
	        }

            if(labelStatsMap.containsKey(labelName)) {
                CDLabelStatistics labelStats = labelStatsMap.get(labelName);
                labelStats.setAggScore(labelStats.getAggScore() + weightedLabelScore);
                labelStats.setMaxScore(Math.max(labelStats.getMaxScore(), labelScore));
            }
            else {
                CDLabelStatistics labelStats = new CDLabelStatistics(labelName, weightedLabelScore, labelScore);
                labelStatsMap.put(labelName, labelStats);
            }
        }

        return labelStatsMap;
    }

    /**
     * Choose the label with the highest aggregated values from the neighbors.  (EQ 2 step 2).
     * @return the chosen label
     */
    private String chooseLabel(Map<String, CDLabelStatistics> labelStatsMap) {
        float maxAggScore = Float.NEGATIVE_INFINITY;
        String chosenLabel;

        float epsilon = 0.00001f;

        // chose max score label or random tie break
        List<String> potentialLabels = new ArrayList<String>();

        for(CDLabelStatistics labelStats : labelStatsMap.values()) {
            float aggScore = labelStats.getAggScore();

            if ((aggScore - maxAggScore) > epsilon ) {
                maxAggScore = aggScore;

                potentialLabels.clear();
                potentialLabels.add(labelStats.getLabelName());
            } else if (Math.abs(maxAggScore - aggScore) < epsilon) {
                potentialLabels.add(labelStats.getLabelName());
            }
        }

        // random tie break
        //int labelIndex = (new Random()).nextInt(potentialLabels.size());
        //chosenLabel = potentialLabels.get(labelIndex);

        // for experiment comparasion, chooose the smallest label name for tie break;
        chosenLabel = potentialLabels.get(0);
        for(String label : potentialLabels) {
            if(Long.parseLong(label) < Long.parseLong(chosenLabel)) {
                chosenLabel = label;
            }
        }

        return chosenLabel;
    }

    /**
     * Calculate the attenuated score of the new label (EQ 3)
     * @return the new label score
     */
    private float getChosenLabelScore(Map<String, CDLabelStatistics> labelStatsMap, String chosenLabel, String oldLabel) {
        float chosenLabelMaxScore = labelStatsMap.get(chosenLabel).getMaxScore();
        float delta = 0;
        if (!chosenLabel.equals(oldLabel))
            delta = hopAttenuation;

        return chosenLabelMaxScore - delta;
    }

    @Override
    public boolean programProperties(Properties properties) {
        hopAttenuation = Float.parseFloat(properties.getProperty(CommunityDetectionConfiguration.HOP_ATTENUATION_KEY));
        nodePreference = Float.parseFloat(properties.getProperty(CommunityDetectionConfiguration.NODE_PREFERENCE_KEY));
        maxIterations = Integer.parseInt(properties.getProperty(CommunityDetectionConfiguration.MAX_ITERATIONS_KEY));
        return true;
    }

}