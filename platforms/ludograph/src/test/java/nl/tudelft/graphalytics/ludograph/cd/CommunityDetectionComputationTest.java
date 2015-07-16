package nl.tudelft.graphalytics.ludograph.cd;

import nl.tudelft.graphalytics.domain.algorithms.CommunityDetectionParameters;
import nl.tudelft.graphalytics.ludograph.GraphalyticsPregelTest;
import nl.tudelft.graphalytics.ludograph.cd.format.CDRecordWriter;
import nl.tudelft.graphalytics.ludograph.cd.format.DirectedCDTextInputFormat;
import nl.tudelft.graphalytics.ludograph.cd.format.UndirectedCDTextInputFormat;
import nl.tudelft.graphalytics.validation.GraphStructure;
import nl.tudelft.graphalytics.validation.cd.CommunityDetectionOutput;
import nl.tudelft.graphalytics.validation.cd.CommunityDetectionValidationTest;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.tudelft.ludograph.conf.LudographConfiguration;
import org.tudelft.ludograph.conf.LudographJobConfiguration;
import org.tudelft.ludograph.graph.PregelParallelizationUnit;
import org.tudelft.ludograph.test.PregelTest;
import org.tudelft.ludograph.test.PregelTestOutput;

import java.io.File;
import java.util.Map;

/**
 * Implementation of the CommunityDetectionValidationTest for LudoGraph
 * It uses the `DirectedCDTextInputFormat` and `UndirectedCDTextInputFormat`
 * And a different `RecordWriter`, `CDRecordWriter` to write out the value of the label.
 *
 * @author Sietse T. Au
 */
public class CommunityDetectionComputationTest extends CommunityDetectionValidationTest {

    public CommunityDetectionOutput executeCommunityDetection(GraphStructure graph, Class<? extends PregelParallelizationUnit<LongWritable, CDLabel, NullWritable, Text>> clazz, Object parameters, boolean isDirected) throws Exception {
        PregelTest<LongWritable, CDLabel, NullWritable, Text> pTest = new GraphalyticsPregelTest<LongWritable, CDLabel, NullWritable, Text>(
                clazz,
                graph,
                new CommunityDetectionConfiguration(),
                parameters,
                isDirected) {

            @Override
            protected LudographJobConfiguration extraConfiguration(LudographJobConfiguration conf) {
                conf = super.extraConfiguration(conf);
                if (isDirected) {
                    conf.set(LudographConfiguration.INPUT_FORMAT_CLASS, DirectedCDTextInputFormat.class.getName());
                } else {
                    conf.set(LudographConfiguration.INPUT_FORMAT_CLASS, UndirectedCDTextInputFormat.class.getName());
                }

                conf.set(LudographConfiguration.RECORD_WRITER_CLASS, CDRecordWriter.class.getName());
                return conf;
            }
        };

        final PregelTestOutput pregelTestOutput = pTest.testPregel(1);
        final File outputDir = pregelTestOutput.getOutput();
        final Map<Long, Long> longLongMap = GraphalyticsPregelTest.vertexLongValueMap(outputDir);
        return new CommunityDetectionOutput(longLongMap);
    }

    @Override
    public CommunityDetectionOutput executeDirectedCommunityDetection(GraphStructure graph, CommunityDetectionParameters parameters) throws Exception {
        return executeCommunityDetection(graph, DirectedCommunityDetectionComputation.class, parameters, true);
    }

    @Override
    public CommunityDetectionOutput executeUndirectedCommunityDetection(GraphStructure graph, CommunityDetectionParameters parameters) throws Exception {
        return executeCommunityDetection(graph, UndirectedCommunityDetectionComputation.class, parameters, false);
    }
}