package nl.tudelft.graphalytics.ludograph;

import com.google.common.collect.ArrayListMultimap;
import nl.tudelft.graphalytics.validation.GraphStructure;
import nl.tudelft.graphalytics.validation.GraphValues;
import nl.tudelft.graphalytics.validation.io.GraphParser;
import nl.tudelft.graphalytics.validation.io.GraphValueParser;
import nl.tudelft.graphalytics.validation.io.LongParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.tudelft.ludograph.conf.LudographConfiguration;
import org.tudelft.ludograph.conf.LudographJobConfiguration;
import org.tudelft.ludograph.graph.PregelParallelizationUnit;
import org.tudelft.ludograph.io.fs.format.FSTextInputFormatNewDirected;
import org.tudelft.ludograph.io.fs.format.FSTextInputFormatNewUndirected;
import org.tudelft.ludograph.io.hdfs.format.FSNoLengthOutputFormat;
import org.tudelft.ludograph.io.hdfs.record.DirectedVertexCheckpointRecordReader;
import org.tudelft.ludograph.io.hdfs.record.DirectedVertexCheckpointRecordWriter;
import org.tudelft.ludograph.io.hdfs.record.IdValueStringRecordWriter;
import org.tudelft.ludograph.io.hdfs.record.UndirectedVertexRecordReader;
import org.tudelft.ludograph.test.PregelTest;

import java.io.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Vector;

/**
 * This class extends the PregelTest class to provide some convenience methods for the validation tests
 */
public class GraphalyticsPregelTest<I extends WritableComparable<? super I>, V extends Writable, E extends Writable, M extends Writable> extends PregelTest<I,V,E,M> {

    protected final Class<? extends PregelParallelizationUnit<I, V, E, M>> programClass;
    protected final GraphStructure graph;
    protected final boolean isDirected;
    protected final Configuration jobParametersConf;

    public GraphalyticsPregelTest(
            Class<? extends PregelParallelizationUnit<I, V, E, M>> programClass,
            GraphStructure graph, ParameterConfiguration paramConf,
            Object parameters,
            boolean isDirected) {
        this.programClass = programClass;
        this.graph = graph;
        this.isDirected = isDirected;
        this.jobParametersConf = paramConf.toConfiguration(parameters);
    }

    /**
     * Reads output from `outputDir` and parses this into a vertex id to `T` mapping.
     *
     * @param outputDir Output directory
     * @param graphValueParser the parser used to parse the vertex value
     * @param <T> the type of the vertex value
     * @return mapping of `Long` to `T`
     * @throws IOException
     */
    public static <T> Map<Long, T> vertexValueMap(File outputDir, GraphValueParser<T> graphValueParser) throws IOException {
        final InputStream inputStream = graphOutputInputStream(outputDir);
        GraphValues<T> outputGraph = GraphParser.parseGraphValuesFromDataset(
                inputStream, graphValueParser);

        Map<Long, T> pathLengths = new HashMap<>();
        for (Long vertex : outputGraph.getVertices()) {
            pathLengths.put(vertex, outputGraph.getVertexValue(vertex));
        }

        inputStream.close();
        return pathLengths;
    }

    /**
     * @see GraphalyticsPregelTest#vertexValueMap(File, GraphValueParser)
     * @throws IOException
     */
    public static Map<Long, Long> vertexLongValueMap(File outputDir) throws IOException {
        return vertexValueMap(outputDir, new LongParser());
    }

    public static InputStream graphOutputInputStream(File outputDir) throws IOException {
        final FileSystem fs = FileSystem.get(new Configuration());
        final Vector<InputStream> inputstreams = new Vector<>();
        for (FileStatus part :  fs.listStatus(new Path(outputDir.toString())) ) {
            inputstreams.add(fs.open(part.getPath()));
        }

        SequenceInputStream seqIs = new SequenceInputStream(inputstreams.elements());


        return seqIs;
    }

    @Override
    protected Class<? extends PregelParallelizationUnit<I, V, E, M>> getProgramClass() {
        return programClass;
    }

    protected LudographJobConfiguration extraConfiguration(LudographJobConfiguration conf) {
        conf.set(LudographConfiguration.RECORD_READER_CLASS, UndirectedVertexRecordReader.class.getName()); // unused
        conf.set(LudographConfiguration.RECORD_WRITER_CLASS, IdValueStringRecordWriter.class.getName());
        conf.set(LudographConfiguration.CHECKPOINT_RECORD_READER_CLASS, DirectedVertexCheckpointRecordReader.class.getName()); // unused
        conf.set(LudographConfiguration.CHECKPOINT_RECORD_WRITER_CLASS, DirectedVertexCheckpointRecordWriter.class.getName()); // unused

        if (isDirected) {
            conf.set(LudographConfiguration.INPUT_FORMAT_CLASS, FSTextInputFormatNewDirected.class.getName());
        } else {
            conf.set(LudographConfiguration.INPUT_FORMAT_CLASS, FSTextInputFormatNewUndirected.class.getName());
        }
        conf.set(LudographConfiguration.OUTPUT_FORMAT_CLASS, FSNoLengthOutputFormat.class.getName());
        return conf;
    }

    @Override
    protected File createDataSet() throws Exception {
        String datasetRaw;
        if (isDirected) {
            datasetRaw = createDirectedDataset();
        } else {
            datasetRaw = createUndirectedDataset();
        }
        File tmpFile = File.createTempFile("test","dataset");
        tmpFile.deleteOnExit();
        FileOutputStream fileOutputStream = new FileOutputStream(tmpFile);
        fileOutputStream.write(datasetRaw.getBytes());
        fileOutputStream.flush();
        fileOutputStream.close();
        return tmpFile;
    }

    private String createDirectedDataset() {
        ArrayListMultimap<Long, Long> incoming = ArrayListMultimap.create();
        ArrayListMultimap<Long, Long> outgoing = ArrayListMultimap.create();

        StringBuilder sb = new StringBuilder();

        for (Long vertex : graph.getVertices()) {
            for (Long otherVertex : graph.getEdgesForVertex(vertex)) {
                outgoing.put(vertex, otherVertex);
                incoming.put(otherVertex, vertex);
            }
        }

        for (Long vertex : graph.getVertices()) {
            sb.append(vertex);
            sb.append(" # ");

            java.util.List<Long> incomingVertices = incoming.get(vertex);
            for (int i = 0; i < incomingVertices.size(); i++) {
                if (i > 0) {
                    sb.append(",");
                }

                Long iV = incomingVertices.get(i);
                sb.append(iV);
            }

            sb.append(" @ ");

            java.util.List<Long> outgoingVertices = outgoing.get(vertex);
            for (int i = 0; i < outgoingVertices.size(); i++) {
                if (i > 0) {
                    sb.append(",");
                }

                Long oV = outgoingVertices.get(i);
                sb.append(oV);
            }

            sb.append("\n");
        }
        return sb.toString();
    }

    private String createUndirectedDataset() {
        StringBuilder sb = new StringBuilder();

        for (Long vertex : graph.getVertices()) {
            sb.append(vertex);

            for (Long otherVertex : graph.getEdgesForVertex(vertex)) {
                sb.append(" ").append(otherVertex);
            }

            sb.append("\n");
        }

        return sb.toString();
    }

    @Override
    protected void setJobProperties(LudographJobConfiguration conf) {
        conf.addResource(jobParametersConf);
    }
}
