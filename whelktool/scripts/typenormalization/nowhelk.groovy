/*
This script fakes input from a source jsonlines file, and writes results to another.

It requires test data files in DATADIR:

    $ mkdir /var/tmp/kb
    $ cd /var/tmp/kb/

Containing two files, with a shared `$basename`, and assumed to end with:

- "$basename-works.jsonl.gz"
- "$basename-instances.jsonl.gz"

To create these, see <./dump-lddb-excerpts.sh>.

Dry-run this script and rely on defaults to find the files:

$ time java -Dxl.secret.properties=../LXD-secret.properties -jar build/libs/whelktool.jar --dry-run scripts/typenormalization/nowhelk.groovy

Dry-run with specific files (passed via java properties):

$ time java -Dxl.secret.properties=../LXD-secret.properties -Dnowhelk.datadir=/var/tmp/kb -Dnowhelk.basename=ssb_partial -jar build/libs/whelktool.jar --dry-run scripts/typenormalization/nowhelk.groovy

*/
import java.util.zip.GZIPInputStream

import static whelk.util.Jackson.mapper

var DATADIR = System.properties['nowhelk.datadir'] ?: '/var/tmp/kb'
var basename = System.properties['nowhelk.basename'] ?: 'lddb-examples'

var workDataFileName = "$DATADIR/${basename}-works.jsonl.gz"
var inDataFileName = "$DATADIR/${basename}-instances.jsonl.gz"
var outDataFileName = "$DATADIR/${basename}-normalized.jsonl"

System.err.println inDataFileName
System.err.println outDataFileName
System.err.println workDataFileName

Closure normalizeTypes = script("${System.properties['typenormalization'] ?: 'simple-types-algorithm'}.groovy")

Map workItems = [:]

try (
  var workDataInStream = new GZIPInputStream(new FileInputStream(workDataFileName))
) {
  workDataInStream.eachLine { line ->
    Map workData = mapper.readValue(line, Map)
    var workItem = [graph: workData[GRAPH], scheduleSave: {}]
    String workId = workItem.graph[1][ID]
    workItems[workId] = workItem
  }
  println "Cached ${workItems.size()} work items."
}

Closure loadWorkItem = { String workId, Closure process ->
  process workItems[workId]
}

try (
  var bibOutput = new PrintWriter(new File(outDataFileName))
  var bibDataInStream = new GZIPInputStream(new FileInputStream(inDataFileName))
) {
  int i = 0
  bibDataInStream.eachLine { line ->
    def data = mapper.readValue(line, Map)
    def item = [graph: data[GRAPH], scheduleSave: {}]
    normalizeTypes(item, loadWorkItem)

    // TODO: Embedding works is OK for debugging purposes; but save to separate stream?
    def thing = item.graph[1]
    if (thing.instanceOf) {
      def workId = thing.instanceOf[ID]
      if (workId) {
        if (workId in workItems) {
          thing.instanceOf = workItems[workId].graph[1]
        } else {
          println "Instance ${thing[ID]} is missing linked work: ${workId}"
        }
      }
    }

    bibOutput.println mapper.writeValueAsString(data)
    i++
  }

  println "Normalized ${i} test records."
} catch (Exception e) {
  e.printStackTrace()
}
