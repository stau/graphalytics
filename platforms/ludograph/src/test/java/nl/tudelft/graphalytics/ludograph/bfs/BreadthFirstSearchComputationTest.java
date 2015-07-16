package nl.tudelft.graphalytics.ludograph.bfs;

import nl.tudelft.graphalytics.domain.algorithms.BreadthFirstSearchParameters;
import nl.tudelft.graphalytics.ludograph.GraphalyticsPregelTest;
import nl.tudelft.graphalytics.validation.GraphStructure;
import nl.tudelft.graphalytics.validation.bfs.BreadthFirstSearchOutput;
import nl.tudelft.graphalytics.validation.bfs.BreadthFirstSearchValidationTest;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.tudelft.ludograph.test.PregelTest;
import org.tudelft.ludograph.test.PregelTestOutput;

import java.io.File;

/**
 * BreadthFirstSearchValidationTest implementation for LudoGraph
 *
 * @author Sietse T. Au
 */
public class BreadthFirstSearchComputationTest extends BreadthFirstSearchValidationTest {

    public BreadthFirstSearchOutput executeBreadthFirstSearch(final GraphStructure graph, BreadthFirstSearchParameters parameters, boolean isDirected) throws Exception {
        PregelTest<LongWritable, LongWritable, NullWritable, LongWritable> pTest = new GraphalyticsPregelTest<>(
                BreadthFirstSearchComputation.class,
                graph,
                new BreadthFirstSearchConfiguration(),
                parameters,
                isDirected);

        final PregelTestOutput pregelTestOutput = pTest.testPregel(2);
        final File outputDir = pregelTestOutput.getOutput();
        return new BreadthFirstSearchOutput(GraphalyticsPregelTest.vertexLongValueMap(outputDir));
    }

    @Override
    public BreadthFirstSearchOutput executeDirectedBreadthFirstSearch(final GraphStructure graph, BreadthFirstSearchParameters parameters) throws Exception {
        return executeBreadthFirstSearch(graph, parameters, true);
    }

    @Override
    public BreadthFirstSearchOutput executeUndirectedBreadthFirstSearch(GraphStructure graph, BreadthFirstSearchParameters parameters) throws Exception {
        return executeBreadthFirstSearch(graph, parameters, false);
    }



}