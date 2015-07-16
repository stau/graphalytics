package nl.tudelft.graphalytics.ludograph;

import org.apache.hadoop.conf.Configuration;

/**
 * Configuration interface to convert parameters to LudoGraph configuration
 */
public interface ParameterConfiguration {

    /**
     * Adapts parameters to Configuration
     *
     * @param parameters
     * @return
     */
    Configuration toConfiguration(Object parameters);
}
