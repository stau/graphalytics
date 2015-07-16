package nl.tudelft.graphalytics.ludograph.cd.format

import java.util.StringTokenizer

import nl.tudelft.graphalytics.ludograph.cd.CDLabel
import org.apache.hadoop.io._
import org.tudelft.ludograph.SerializedCollection
import org.tudelft.ludograph.graph.PregelParallelizationUnit
import org.tudelft.ludograph.graph.graph.vertices.NewUndirectedVertex
import org.tudelft.ludograph.graph.graph.vertices.mock.Edge
import org.tudelft.ludograph.io.LudographIOFactory
import org.tudelft.ludograph.io.fs.format.FSTextInputFormat
import org.tudelft.ludograph.utils.formatreaders.UndirectedVertexFormatReader

class UndirectedCDTextInputFormat(conf : LudographIOFactory) extends FSTextInputFormat[LongWritable, CDLabel, NullWritable, Text](conf) {

  private[this] var _formatReader: UndirectedVertexFormatReader[LongWritable, CDLabel, NullWritable, Text] = null

  override def formatReader(clazzI: Class[LongWritable], clazzV: Class[CDLabel], clazzE: Class[NullWritable]): UndirectedVertexFormatReader[LongWritable, CDLabel, NullWritable, Text] = {
    Option(_formatReader) match {
      case Some(f) => f
      case None =>
        _formatReader = new UndirectedVertexFormatReader[LongWritable, CDLabel, NullWritable, Text] {

          override def read(line: String, pu: PregelParallelizationUnit[LongWritable, CDLabel, NullWritable, Text]): Unit = {
            val nPu = pu.asInstanceOf[NewUndirectedVertex[LongWritable, CDLabel, NullWritable, Text]]
            val tokenizer: StringTokenizer = new StringTokenizer(line, " \t,")
            if (tokenizer.hasMoreElements) {
              // id
              val idToken = tokenizer.nextToken()
              val id = getId(idToken)
              val vertex = getVertex(idToken)
              pu.setId(id); // "id\t"
              pu.setValue(vertex)

              // store bidirectional-edges
              var edgeDstIdToken : String = null
              while (tokenizer.hasMoreElements) {
                edgeDstIdToken = tokenizer.nextToken()
                val edgeDstId = getId(edgeDstIdToken); // edge dst
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