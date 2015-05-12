/**
 * Copyright 2015 Delft University of Technology
 * <p/>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package nl.tudelft.graphalytics.graphlab;

import nl.tudelft.graphalytics.PlatformExecutionException;
import nl.tudelft.graphalytics.domain.Algorithm;
import nl.tudelft.graphalytics.domain.Graph;
import nl.tudelft.graphalytics.domain.GraphFormat;
import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.exec.PumpStreamHandler;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Template Test class for a GraphLab algorithm.
 *
 * @author Jorai Rijsdijk
 */
public abstract class AlgorithmTest {
    protected static GraphLabPlatform graphLab = new GraphLabPlatform();
    private static final String BASE_PATH = AlgorithmTest.class.getResource("/").getPath();

    /**
     * Perform an algorithm test.
     *
     * @param algorithm     The algorithm to test
     * @param prefix        The prefix of the algorithm input/output files
     * @param algorithmFile The python file of the test
     * @param parameters    The algorithm parameters
     * @param directed      Whether the input is directed
     * @param edgeBased     Whether the input format is edge based
     */
    protected void performTest(Algorithm algorithm, String prefix, String algorithmFile, Object parameters, boolean directed, boolean edgeBased) {
        performTest(algorithm, prefix, algorithmFile, parameters, directed, edgeBased, false);
    }

    /**
     * Perform an algorithm test.
     *
     * @param algorithm      The algorithm to test
     * @param prefix         The prefix of the algorithm input/output files
     * @param algorithmFile  The python file of the test
     * @param parameters     The algorithm parameters
     * @param directed       Whether the input is directed
     * @param edgeBased      Whether the input format is edge based
     * @param useOriginal    Whether to pass the original file as expected output
     *                       This is useful if output needs to be compared to the input graph instead of the expected output
     * @param extraArguments Extra arguments that should be passed to the test python file
     */
    protected void performTest(Algorithm algorithm, String prefix, String algorithmFile, Object parameters, boolean directed, boolean edgeBased, boolean useOriginal, String... extraArguments) {
        graphLab.setSaveGraphResult(true);
        String graphFile = "test-examples/" + prefix + "-input";
        Graph graph = new Graph(prefix + "-input", graphFile, new GraphFormat(directed, edgeBased), 1, 1);
        try {
            graphLab.uploadGraph(graph, BASE_PATH + graphFile);
        } catch (Exception e) {
            fail("Unable to upload graph" + (e.getMessage() != null ? ": " + e.getMessage() : ""));
            e.printStackTrace();
        }

        try {
            graphLab.executeAlgorithmOnGraph(algorithm, graph, parameters);
        } catch (PlatformExecutionException e) {
            fail("Algorithm execution failed" + (e.getMessage() != null ? ": " + e.getMessage() : ""));
            e.printStackTrace();
        }

        File testScriptFile = new File(BASE_PATH, "nl/tudelft/graphalytics/graphlab/" + algorithmFile);
        String outputFile = BASE_PATH + "test-examples/" + prefix + (useOriginal ? "input" : "output");
        assertTrue(executeTestScript(testScriptFile, "target/" + algorithm.toString().toLowerCase() + "_" + graph.getName(), outputFile, extraArguments));
    }

    protected boolean executeTestScript(File scriptFile, String graphFile, String outputFile, String... extraArguments) {
        if (!scriptFile.exists()) {
            throw new IllegalArgumentException("Cannot find GraphLab Test script: " + scriptFile.getAbsolutePath());
        }

        CommandLine commandLine = new CommandLine("python2");
        commandLine.addArgument(scriptFile.getAbsolutePath());
        commandLine.addArgument(graphFile);
        commandLine.addArgument(outputFile);
        if (extraArguments.length > 0) {
            commandLine.addArguments(extraArguments);
        }

        // Set the executor of the command, if desired this can be changed to a custom implementation
        DefaultExecutor executor = new DefaultExecutor();

        // Set the OutputStream to enable printing the output of the algorithm
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        executor.setStreamHandler(new PumpStreamHandler(outputStream));

        try {
            // Execute the actual command and store the return code
            executor.execute(commandLine);
            // Print the command output
            System.out.println(outputStream.toString());
            return true;
        } catch (IOException e) {
            // Catch the exception thrown when the process exits with result != 0 or another IOException occurs
            System.out.println(outputStream.toString());
            return false;
        }
    }
}
