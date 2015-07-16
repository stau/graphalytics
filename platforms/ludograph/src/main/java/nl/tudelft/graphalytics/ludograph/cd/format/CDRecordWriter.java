package nl.tudelft.graphalytics.ludograph.cd.format;

import nl.tudelft.graphalytics.ludograph.cd.CDLabel;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.tudelft.ludograph.graph.PregelParallelizationUnit;
import org.tudelft.ludograph.io.record.PregelParallelizationUnitRecordWriter;

import java.io.DataOutput;
import java.io.IOException;

/**
 * A record writer which writes the PU to the specified format:
 * ID LABEL
 *
 * @author Sietse T. Au
 */
public class CDRecordWriter extends PregelParallelizationUnitRecordWriter<LongWritable, CDLabel, NullWritable, Text> {
    @Override
    public void writePU(DataOutput output, PregelParallelizationUnit<LongWritable, CDLabel, NullWritable, Text> pu) throws IOException {
        output.writeUTF(pu.getId().get() + " " + pu.getValue().getLabelName().toString() + "\n");
    }

    @Override
    public void writePuHeader(DataOutput output) throws IOException {

    }

    @Override
    public PregelParallelizationUnitRecordWriter<LongWritable, CDLabel, NullWritable, Text> createTypedRecordWriter(Class<LongWritable> idClass, Class<CDLabel> valueClass, Class<NullWritable> edgeValueClass, Class<Text> msgClass) throws IOException, ClassNotFoundException, InstantiationException, IllegalAccessException {
        return new CDRecordWriter();
    }
}
