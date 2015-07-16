package nl.tudelft.graphalytics.ludograph.cd.format

import java.util.StringTokenizer

import nl.tudelft.graphalytics.ludograph.cd.CDLabel
import org.apache.hadoop.io._
import org.tudelft.ludograph.SerializedCollection
import org.tudelft.ludograph.graph.PregelParallelizationUnit
import org.tudelft.ludograph.graph.graph.vertices.NewDirectedVertex
import org.tudelft.ludograph.graph.graph.vertices.mock.Edge
import org.tudelft.ludograph.io.LudographIOFactory
import org.tudelft.ludograph.io.fs.format.FSTextInputFormat
import org.tudelft.ludograph.utils.formatreaders.DirectedVertexFormatReader

class DirectedCDTextInputFormat(conf : LudographIOFactory) extends FSTextInputFormat[LongWritable, CDLabel, NullWritable, Text](conf) {

  private[this] var _formatReader: DirectedVertexFormatReader[LongWritable, CDLabel, NullWritable, Text] = null

  override def formatReader(clazzI: Class[LongWritable], clazzV: Class[CDLabel], clazzE: Class[NullWritable]): DirectedVertexFormatReader[LongWritable, CDLabel, NullWritable, Text] = {
    Option(_formatReader) match {
      case Some(f) => f
      case None =>
        _formatReader = new DirectedVertexFormatReader[LongWritable, CDLabel, NullWritable, Text] {

          override def read(line: String, pu: PregelParallelizationUnit[LongWritable, CDLabel, NullWritable, Text]): Unit = {
            val nPu = pu.asInstanceOf[NewDirectedVertex[LongWritable, CDLabel, NullWritable, Text]]
            val tokenizer: StringTokenizer = new StringTokenizer(line, "#@")
            if (tokenizer.hasMoreElements) {
              val id = new StringTokenizer(tokenizer.nextToken(), " \t\n\r\f,.:;?![]'").nextToken()

              // id
              pu.setId(getId(id));
              pu.setValue(getVertex(id))

              // IN
              var edgeTokenizer = new StringTokenizer(tokenizer.nextToken(), " \t\n\r\f,.:;?![]'")
              while (edgeTokenizer.hasMoreElements) {
                /* read and discard in edge */
                val edgeSrcId = getId(edgeTokenizer.nextToken())
                nPu.getIncoming.add(new Edge[LongWritable, NullWritable](edgeSrcId, getEdgeValue()))
              }

              // store out edges
              edgeTokenizer = new StringTokenizer(tokenizer.nextToken(), " \t\n\r\f,.:;?![]'")
              while (edgeTokenizer.hasMoreElements) {
                val edgeDstId = getId(edgeTokenizer.nextToken()); // out edge dst
                nPu.getOutgoing.add(new Edge[LongWritable, NullWritable](edgeDstId, getEdgeValue()))
              }

              if (nPu.getOutgoing.isInstanceOf[SerializedCollection[PregelParallelizationUnit[LongWritable, CDLabel, NullWritable, Text]]]) {
                nPu.getOutgoing.asInstanceOf[SerializedCollection[PregelParallelizationUnit[LongWritable, CDLabel, NullWritable, Text]]].trimInternalBuffer()
              }
            }
          }

          override def getId(t: String): LongWritable = new LongWritable(t.toLong)

          override def getEdgeValue: NullWritable = NullWritable.get()

          override def getVertex(t: String): CDLabel = null
        }
        _formatReader
    }

  }
}