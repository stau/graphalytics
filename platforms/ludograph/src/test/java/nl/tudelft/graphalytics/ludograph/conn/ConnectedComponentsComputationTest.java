package nl.tudelft.graphalytics.ludograph.conn;

import nl.tudelft.graphalytics.ludograph.GraphalyticsPregelTest;
import nl.tudelft.graphalytics.ludograph.ParameterConfiguration;
import nl.tudelft.graphalytics.validation.GraphStructure;
import nl.tudelft.graphalytics.validation.conn.ConnectedComponentsOutput;
import nl.tudelft.graphalytics.validation.conn.ConnectedComponentsValidationTest;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.tudelft.ludograph.graph.PregelParallelizationUnit;
import org.tudelft.ludograph.test.PregelTest;
import org.tudelft.ludograph.test.PregelTestOutput;

import java.io.File;

/**
 * ConnectedComponentsValidationTest implementation for LudoGraph
 *
 * @author Sietse T. Au
 */
public class ConnectedComponentsComputationTest extends ConnectedComponentsValidationTest {

    public ConnectedComponentsOutput executeConnectedComponents(GraphStructure graph, Class<? extends PregelParallelizationUnit<LongWritable, LongWritable, NullWritable, LongWritable>> clazz, boolean isDirected) throws Exception {
        PregelTest<LongWritable, LongWritable, NullWritable, LongWritable> pTest = new GraphalyticsPregelTest<>(
                clazz,
                graph,
                new ParameterConfiguration() {
                    @Override
                    public Configuration toConfiguration(Object parameters) {
                        return new Configuration(false);
                    }
                },
                new Object(),
                isDirected);

        final PregelTestOutput pregelTestOutput = pTest.testPregel(1);
        final File outputDir = pregelTestOutput.getOutput();
        return new ConnectedComponentsOutput(GraphalyticsPregelTest.vertexLongValueMap(outputDir));
    }

    @Override
    public ConnectedComponentsOutput executeDirectedConnectedComponents(GraphStructure graph) throws Exception {
        return executeConnectedComponents(graph, DirectedConnectedComponentsComputation.class, true);
    }

    @Override
    public ConnectedComponentsOutput executeUndirectedConnectedComponents(GraphStructure graph) throws Exception {
        return executeConnectedComponents(graph, UndirectedConnectedComponentsComputation.class, false);
    }
}