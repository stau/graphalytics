package nl.tudelft.graphalytics.ludograph;

import nl.tudelft.graphalytics.domain.Graph;
import org.apache.hadoop.conf.Configuration;
import org.tudelft.ludograph.cloud.scheduler.joblevel.DisabledJobLevelScheduler;
import org.tudelft.ludograph.conf.LudographConfiguration;
import org.tudelft.ludograph.conf.LudographJobConfiguration;
import org.tudelft.ludograph.io.fs.format.FSTextInputFormatNewDirected;
import org.tudelft.ludograph.io.fs.format.FSTextInputFormatNewUndirected;
import org.tudelft.ludograph.io.hdfs.format.GenericHDFSInputFormat;
import org.tudelft.ludograph.io.hdfs.format.GenericHDFSOutputFormat;
import org.tudelft.ludograph.io.hdfs.record.*;

import java.io.DataInputStream;
import java.io.DataOutputStream;

/**
 * Creates a LudoGraph job configuration given the parameters
 */
public class LudoGraphJobBuilder {

    private Class<?> computationClass;
    private Graph graph;
    private Configuration jobParameters;
    private String inputPath;
    private String outputPath;
    private int numWorkers;
    private int workerMemory;
    private int masterMemory;
    private int workerCores;
    private int masterCores;
    private String platformHost;
    private int platformPort;
    private int clientPort;
    private String jobJarPath;
    private String writerClass;

    public LudoGraphJobBuilder() {

    }

    public LudoGraphJobBuilder computationClass(Class<?> computationClass) {
        this.computationClass = computationClass;
        return this;
    }

    public LudoGraphJobBuilder graph(Graph graph) {
        this.graph = graph;
        return this;
    }

    public LudoGraphJobBuilder jobParameters(Configuration conf) {
        this.jobParameters = conf;
        return this;
    }

    public LudoGraphJobBuilder inputPath(String inputPath) {
        this.inputPath = inputPath;
        return this;
    }

    public LudoGraphJobBuilder outputPath(String outputPath) {
        this.outputPath = outputPath;
        return this;
    }

    public LudoGraphJobBuilder numWorkers(int numWorkers) {
        this.numWorkers = numWorkers;
        return this;
    }

    public LudoGraphJobBuilder workerMemory(int workerMemory) {
        this.workerMemory = workerMemory;
        return this;
    }

    public LudoGraphJobBuilder masterMemory(int masterMemory) {
        this.masterMemory = masterMemory;
        return this;
    }

    public LudoGraphJobBuilder workerCores(int workerCores) {
        this.workerCores = workerCores;
        return this;
    }

    public LudoGraphJobBuilder masterCores(int masterCores) {
        this.masterCores = masterCores;
        return this;
    }

    /**
     * Optional
     */
    public LudoGraphJobBuilder recordWriterClass(Class<?> recordWriterClass) {
        this.writerClass = recordWriterClass.getName();
        return this;
    }


    /**
     * Builds a LudoGraphJob
     *
     * @return `LudoGraphJob`
     */
    public LudoGraphJob build() {
        String readerClass, checkpointReaderClass, checkpointWriterClass, inputFormatClass;
        if (this.graph.getGraphFormat().isDirected()) {
            readerClass = DirectedVertexRecordReader.class.getName();
            if (writerClass != null)
                writerClass = IdValueRecordWriter.class.getName();
            checkpointReaderClass = DirectedVertexCheckpointRecordReader.class.getName();
            checkpointWriterClass = DirectedVertexCheckpointRecordWriter.class.getName();
            inputFormatClass = FSTextInputFormatNewDirected.class.getName();
        } else {
            readerClass = UndirectedVertexRecordReader.class.getName();
            if (writerClass != null)
                writerClass = IdValueRecordWriter.class.getName();
            checkpointReaderClass = DirectedVertexCheckpointRecordReader.class.getName(); // there are only directed checkpoints?
            checkpointWriterClass = DirectedVertexCheckpointRecordWriter.class.getName();
            inputFormatClass = FSTextInputFormatNewUndirected.class.getName();
        }

        Configuration configuration = new Configuration(false);
        configuration.set(LudographConfiguration.DEBUG_GUMBY, "false");
        configuration.set(LudographConfiguration.JOB_PROGRAM, computationClass.getName());
        configuration.set(LudographConfiguration.INPUT_FORMAT_CLASS, inputFormatClass);
        configuration.set(LudographConfiguration.RECORD_READER_CLASS, readerClass);
        configuration.set(LudographConfiguration.INPUT_STREAM_CLASS, DataInputStream.class.getName());
        configuration.set(LudographConfiguration.INPUT_DFS_PATH, inputPath);
        configuration.set(LudographConfiguration.OUTPUT_FORMAT_CLASS, GenericHDFSOutputFormat.class.getName());
        configuration.set(LudographConfiguration.RECORD_WRITER_CLASS,writerClass);
        configuration.set(LudographConfiguration.OUTPUT_STREAM_CLASS, DataOutputStream.class.getName());
        configuration.set(LudographConfiguration.OUTPUT_DFS_PATH, outputPath);
        configuration.set(LudographConfiguration.DEBUG_CHECKPOINT, "false");
        configuration.set(LudographConfiguration.CHECKPOINT_INPUT_FORMAT_CLASS, GenericHDFSInputFormat.class.getName());
        configuration.set(LudographConfiguration.CHECKPOINT_RECORD_READER_CLASS, checkpointReaderClass);
        configuration.set(LudographConfiguration.CHECKPOINT_INPUT_STREAM_CLASS, DataInputStream.class.getName());
        configuration.set(LudographConfiguration.CHECKPOINT_OUTPUT_FORMAT_CLASS,GenericHDFSOutputFormat.class.getName());
        configuration.set(LudographConfiguration.CHECKPOINT_RECORD_WRITER_CLASS,checkpointWriterClass);
        configuration.set(LudographConfiguration.CHECKPOINT_OUTPUT_STREAM_CLASS,DataOutputStream.class.getName());
        configuration.set(LudographJobConfiguration.JOB_LEVEL_SCHEDULER, DisabledJobLevelScheduler.class.getName());
        configuration.addResource(jobParameters);

        final LudoGraphJob ludoGraphJob = new LudoGraphJob();
        ludoGraphJob.setJobConf(configuration);
        ludoGraphJob.setNumWorkers(numWorkers);
        ludoGraphJob.setWorkerMemory(workerMemory);
        ludoGraphJob.setWorkerCores(workerCores);
        ludoGraphJob.setMasterMemory(masterMemory);
        ludoGraphJob.setMasterCores(masterCores);
        ludoGraphJob.setPlatformHost(platformHost);
        ludoGraphJob.setPlatformPort(platformPort);
        ludoGraphJob.setClientPort(clientPort);
        ludoGraphJob.setJobJarPath(jobJarPath);
        return ludoGraphJob;
    }

    public LudoGraphJobBuilder platformHost(String platformHost) {
        this.platformHost = platformHost;
        return this;
    }

    public LudoGraphJobBuilder platformPort(int platformPort) {
        this.platformPort = platformPort;
        return this;
    }

    public LudoGraphJobBuilder clientPort(int clientPort) {
        this.clientPort = clientPort;
        return this;
    }

    public LudoGraphJobBuilder jobJarPath(String jobJarPath) {
        this.jobJarPath = jobJarPath;
        return this;
    }
}
