package nl.tudelft.graphalytics.ludograph.stats;

import nl.tudelft.graphalytics.ludograph.ParameterConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.tudelft.ludograph.conf.LudographConfiguration;

/**
 * Local clustering coefficient configuration, only states the aggregator name.
 *
 * @author Sietse T. Au
 */
public class LocalClusteringCoefficientConfiguration implements ParameterConfiguration{

    public static final String LCC_AGGREGATOR_NAME = "Average LCC";

    @Override
    public Configuration toConfiguration(Object parameters) {
        Configuration conf = new Configuration(false);
        conf.set(LudographConfiguration.AGGREGATOR_NAME, LocalClusteringCoefficientConfiguration.LCC_AGGREGATOR_NAME);
        conf.set(LudographConfiguration.AGGREGATOR_CLASS, DoubleAverageAggregator.class.getName());
        return conf;
    }
}
