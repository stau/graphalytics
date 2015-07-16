package nl.tudelft.graphalytics.ludograph.bfs;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.tudelft.ludograph.graph.graph.vertices.NewDirectedVertex;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Properties;

/**
 * BFS implementation
 *
 * @author Sietse T. Au
 */
public class BreadthFirstSearchComputation  extends NewDirectedVertex<LongWritable, LongWritable, NullWritable, LongWritable> {
    private boolean isVisited;

    private static long initPU;

    public BreadthFirstSearchComputation() {
        super();
    }

    @Override
    public void compute(Iterable<LongWritable> messages) {
        if (this.getSuperStep() == 1) {
            if (this.getId().get() == initPU) {
                this.setValue(new LongWritable(this.getSuperStep() - 1)); // -1 to match unit test
                this.sendToAll(getValue());

                this.isVisited = true;
            } else {
                this.isVisited = false;
            }
        } else {
            if (!this.isVisited) {
                this.setValue(new LongWritable(this.getSuperStep() - 1));
                this.sendToAll(getValue());

                // I'm done
                this.isVisited = true;
            }
        }
        this.setHalt(true);
    }

    @Override
    public boolean programProperties(Properties props) {
        initPU = Long.parseLong(props.getProperty(BreadthFirstSearchConfiguration.SOURCE_VERTEX_KEY));
        this.setValue(new LongWritable(Long.MAX_VALUE)); // default value
        return true;
    }

    /**
     * State for Checkpoint
     */
    @Override
    public void storeState(DataOutput output) throws IOException {
        super.storeState(output);
        output.writeBoolean(this.isVisited);
    }

    @Override
    public void loadState(DataInput input) throws IOException {
        super.loadState(input);
        this.isVisited = input.readBoolean();
    }

    @Override
    public String toString() {
        String result = super.toString() + "\n";

        result += "isVisited: " + this.isVisited + "\n";
        result += "isHalted: " + this.isVisited;

        return result;
    }
}

