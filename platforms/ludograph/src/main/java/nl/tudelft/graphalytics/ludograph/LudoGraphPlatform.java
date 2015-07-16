package nl.tudelft.graphalytics.ludograph;

import nl.tudelft.graphalytics.Platform;
import nl.tudelft.graphalytics.PlatformExecutionException;
import nl.tudelft.graphalytics.domain.Algorithm;
import nl.tudelft.graphalytics.domain.Graph;
import nl.tudelft.graphalytics.domain.NestedConfiguration;
import nl.tudelft.graphalytics.domain.PlatformBenchmarkResult;
import nl.tudelft.graphalytics.ludograph.bfs.BreadthFirstSearchComputation;
import nl.tudelft.graphalytics.ludograph.bfs.BreadthFirstSearchConfiguration;
import nl.tudelft.graphalytics.ludograph.cd.CommunityDetectionConfiguration;
import nl.tudelft.graphalytics.ludograph.cd.DirectedCommunityDetectionComputation;
import nl.tudelft.graphalytics.ludograph.cd.UndirectedCommunityDetectionComputation;
import nl.tudelft.graphalytics.ludograph.conn.DirectedConnectedComponentsComputation;
import nl.tudelft.graphalytics.ludograph.conn.UndirectedConnectedComponentsComputation;
import nl.tudelft.graphalytics.ludograph.stats.DirectedLocalClusteringCoefficientComputation;
import nl.tudelft.graphalytics.ludograph.stats.LocalClusteringCoefficientConfiguration;
import nl.tudelft.graphalytics.ludograph.stats.UndirectedLocalClusteringCoefficientComputation;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.tudelft.ludograph.comm.netty.ServerBindException;
import org.tudelft.ludograph.comm.netty.ServerUnreachableException;

import java.net.UnknownHostException;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

public class LudoGraphPlatform implements Platform {
    private static final Logger LOG = LogManager.getLogger();

    /** Property key for the directory on HDFS in which to store all input and output. */
    public static final String HDFS_DIRECTORY_KEY = "hadoop.hdfs.directory";

    /** Default value for the directory on HDFS in which to store all input and output. */
    public static final String HDFS_DIRECTORY = "graphalytics";

    public static final String NUM_WORKERS = "ludograph.job.num_workers";
    public static final String MEMORY_WORKER = "ludograph.job.worker.memory";
    public static final String MEMORY_MASTER = "ludograph.job.master.memory";
    public static final String CORES_WORKER = "ludograph.job.worker.cores";
    public static final String CORES_MASTER = "ludograph.job.master.cores";
    public static final String PAM_HOST = "ludograph.platform.host";
    public static final String PAM_PORT = "ludograph.platform.port";
    public static final String CLIENT_PORT = "ludograph.client.listen.port";
    public static final String JOB_JAR_PATH = "ludograph.job.jar.path";

    private final Map<String, String> pathsOfGraphs = new HashMap<>();
    private PropertiesConfiguration ludographConfig;
    private String hdfsDirectory;

    public LudoGraphPlatform() {
//        LudographLogging.initializeLogging(); // LudographLogging overrides graphalytics logging
        loadConfiguration();
    }

    private void loadConfiguration() {
        try {
            ludographConfig = new PropertiesConfiguration("ludograph.properties");
        } catch (ConfigurationException e) {
            // Fall-back to an empty properties file
            LOG.info("Could not find or load ludograph.properties.");
            ludographConfig = new PropertiesConfiguration();
        }
        hdfsDirectory = ludographConfig.getString(HDFS_DIRECTORY_KEY, HDFS_DIRECTORY);
    }

    @Override
    public void uploadGraph(Graph graph, String graphFilePath) throws Exception {
        LOG.entry(graph, graphFilePath);

        String uploadPath = Paths.get(hdfsDirectory, getName(), "input", graph.getName()).toString();

        // Upload the graph to HDFS
        FileSystem fs = FileSystem.get(new Configuration());
        fs.copyFromLocalFile(new Path(graphFilePath), new Path(uploadPath));
        fs.close();

        // Track available datasets in a map
        pathsOfGraphs.put(graph.getName(), uploadPath);
    }

    @Override
    public PlatformBenchmarkResult executeAlgorithmOnGraph(Algorithm algorithm, Graph graph, Object parameters) throws PlatformExecutionException {
        System.out.println("executing algorithm");
        String hdfsInputPath = pathsOfGraphs.get(graph.getName());
        String hdfsOutputPath = Paths.get(hdfsDirectory, getName(), "output",
                algorithm + "-" + graph.getName()).toString();

        if (graph.getGraphFormat().isEdgeBased()) {
            throw new PlatformExecutionException("LudoGraph does not support edge-lists");
        }

        Configuration jobConf = null;
        Class<?> computationClass = null;
        switch (algorithm) {
            case BFS:
                jobConf = new BreadthFirstSearchConfiguration().toConfiguration(parameters);
                computationClass = BreadthFirstSearchComputation.class;
                break;
            case CD:
                jobConf = new CommunityDetectionConfiguration().toConfiguration(parameters);
                if (graph.getGraphFormat().isDirected()) {
                    computationClass = DirectedCommunityDetectionComputation.class;
                } else {
                    computationClass = UndirectedCommunityDetectionComputation.class;
                }
                break;
            case CONN:
                jobConf = new Configuration(false);
                if (graph.getGraphFormat().isDirected()) {
                    computationClass = DirectedConnectedComponentsComputation.class;
                } else {
                    computationClass = UndirectedConnectedComponentsComputation.class;
                }
                break;
            case EVO:
                break;
            case STATS:
                jobConf = new LocalClusteringCoefficientConfiguration().toConfiguration(parameters);
                if (graph.getGraphFormat().isDirected()) {
                    computationClass = DirectedLocalClusteringCoefficientComputation.class;
                } else {
                    computationClass = UndirectedLocalClusteringCoefficientComputation.class;
                }
                break;
        }

        if(jobConf == null || computationClass == null) {
            throw new PlatformExecutionException("No jobConf or computation class defined");
        }

        LudoGraphJobBuilder jobBuilder = new LudoGraphJobBuilder();
        LudoGraphJob job = jobBuilder
                .graph(graph)
                .jobParameters(jobConf)
                .computationClass(computationClass)
                .inputPath(hdfsInputPath)
                .outputPath(hdfsOutputPath)
                .numWorkers(ludographConfig.getInt(NUM_WORKERS))
                .workerMemory(ludographConfig.getInt(MEMORY_WORKER))
                .workerCores(ludographConfig.getInt(CORES_WORKER))
                .masterMemory(ludographConfig.getInt(MEMORY_MASTER))
                .masterCores(ludographConfig.getInt(CORES_MASTER))
                .platformHost(ludographConfig.getString(PAM_HOST))
                .platformPort(ludographConfig.getInt(PAM_PORT))
                .clientPort(ludographConfig.getInt(CLIENT_PORT))
                .jobJarPath(ludographConfig.getString(JOB_JAR_PATH))
                .build();

        // run job
        try {
            job.run();
        } catch (UnknownHostException | ServerBindException | InterruptedException | ServerUnreachableException e) {
            LOG.error(e);
            throw new PlatformExecutionException("Something went wrong: ", e);
        }

        return new PlatformBenchmarkResult(NestedConfiguration.empty());
    }

    @Override
    public void deleteGraph(String graphName) {

    }

    @Override
    public String getName() {
        return "ludograph";
    }

    @Override
    public NestedConfiguration getPlatformConfiguration() {
        try {
            org.apache.commons.configuration.Configuration configuration =
                    new PropertiesConfiguration("ludograph.properties");
            return NestedConfiguration.fromExternalConfiguration(configuration, "ludograph.properties");
        } catch (ConfigurationException ex) {
            return NestedConfiguration.empty();
        }
    }
}
