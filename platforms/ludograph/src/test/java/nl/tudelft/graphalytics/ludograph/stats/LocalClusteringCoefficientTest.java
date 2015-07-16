package nl.tudelft.graphalytics.ludograph.stats;

import nl.tudelft.graphalytics.ludograph.GraphalyticsPregelTest;
import nl.tudelft.graphalytics.validation.GraphStructure;
import nl.tudelft.graphalytics.validation.io.DoubleParser;
import nl.tudelft.graphalytics.validation.stats.LocalClusteringCoefficientOutput;
import nl.tudelft.graphalytics.validation.stats.LocalClusteringCoefficientValidationTest;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.tudelft.ludograph.graph.PregelParallelizationUnit;
import org.tudelft.ludograph.test.PregelTest;
import org.tudelft.ludograph.test.PregelTestOutput;

import java.io.File;
import java.util.Map;


/**
 * LocalClusteringCoefficientValidationTest implementation for LudoGraph
 *
 * @author Sietse T. Au
 */
public class LocalClusteringCoefficientTest extends LocalClusteringCoefficientValidationTest {
    public LocalClusteringCoefficientOutput executeLocalClusteringCoefficient(GraphStructure graph, Class<? extends PregelParallelizationUnit<LongWritable, DoubleWritable, NullWritable, LocalClusteringCoefficientMessage>> clazz, boolean isDirected) throws Exception {
        PregelTest<LongWritable, DoubleWritable, NullWritable, LocalClusteringCoefficientMessage> pTest = new GraphalyticsPregelTest<>(
                clazz,
                graph,
                new LocalClusteringCoefficientConfiguration(),
                new Object(),
                isDirected);

        final PregelTestOutput pregelTestOutput = pTest.testPregel(4);
        final File outputDir = pregelTestOutput.getOutput();
        Map<Long, Double> llcs = GraphalyticsPregelTest.vertexValueMap(outputDir, new DoubleParser());
        DoubleAverage doubleAverage = (DoubleAverage) pregelTestOutput.getAggregators().get(LocalClusteringCoefficientConfiguration.LCC_AGGREGATOR_NAME).getValue();
        return new LocalClusteringCoefficientOutput(llcs, doubleAverage.get());
    }

    @Override
    public LocalClusteringCoefficientOutput executeDirectedLocalClusteringCoefficient(GraphStructure graph) throws Exception {
        return executeLocalClusteringCoefficient(graph, DirectedLocalClusteringCoefficientComputation.class, true);
    }

    @Override
    public LocalClusteringCoefficientOutput executeUndirectedLocalClusteringCoefficient(GraphStructure graph) throws Exception {
        return executeLocalClusteringCoefficient(graph, UndirectedLocalClusteringCoefficientComputation.class, false);
    }
}
