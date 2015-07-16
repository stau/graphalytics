package nl.tudelft.graphalytics.ludograph.bfs;

import nl.tudelft.graphalytics.domain.algorithms.BreadthFirstSearchParameters;
import nl.tudelft.graphalytics.ludograph.ParameterConfiguration;
import org.apache.hadoop.conf.Configuration;

/**
 * BFS configuration, SOURCE_VERTEX_KEY is the seed vertex.
 *
 * @author Sietse T. Au
 */
public class BreadthFirstSearchConfiguration implements ParameterConfiguration {
    public static final String SOURCE_VERTEX_KEY = "bfs.init.id";

    @Override
    public Configuration toConfiguration(Object parameters) {
        BreadthFirstSearchParameters bfsParams = (BreadthFirstSearchParameters) parameters;
        Configuration configuration = new Configuration(false);
        configuration.setLong(SOURCE_VERTEX_KEY, bfsParams.getSourceVertex());
        return configuration;
    }
}
