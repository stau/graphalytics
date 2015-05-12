package nl.tudelft.graphalytics.graphlab.evo;

import nl.tudelft.graphalytics.domain.Algorithm;
import nl.tudelft.graphalytics.domain.algorithms.ForestFireModelParameters;
import nl.tudelft.graphalytics.graphlab.AlgorithmTest;
import org.junit.Test;

public class ForestFireModelTest extends AlgorithmTest {
    @Test
    public void testDirectedConnectedComponents() {
        performTest(Algorithm.EVO, "evo-dir", "evo/ForestFireModelTest.py",
                new ForestFireModelParameters(50, 0.5f, 0.5f, 2, 5), true, true, true, "51", "2", "5");
    }

    @Test
    public void testUndirectedConnectedComponents() {
        performTest(Algorithm.EVO, "evo-undir", "evo/ForestFireModelTest.py",
                new ForestFireModelParameters(50, 0.5f, 0.5f, 2, 5), false, true, true, "51", "2", "5");
    }
}
