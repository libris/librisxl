/*
This script fakes input from a source jsonlines file, and writes results to another.

It requires test data files in DATADIR:

$ mkdir /var/tmp/kb
$ cd /var/tmp/kb/

## Instances:
$ psql -h PGSQL_HOST -U PGSQL_USER whelk -tc "copy (select lddb.data from lddb, lddb__dependencies itemof, lddb__dependencies heldby where lddb.id = itemof.dependsonid and itemof.relation = 'itemOf' and heldby.relation = 'heldBy' and itemof.id = heldby.id and heldby.dependsonid in (select id from lddb__identifiers where iri in ('https://libris.kb.se/library/Ssb', 'https://libris.kb.se/library/SsbE'))) to stdout;" | sed 's/\\\\/\\/g' | gzip > stg-lddb-bib-heldbyssb.jsonl.gz

## Works:
$ psql -h PGSQL_HOST -U PGSQL_USER whelk -tc "copy (select lddb.data from lddb, lddb__dependencies instanceof, lddb__dependencies itemof, lddb__dependencies heldby where lddb.id = instanceof.dependsonid and instanceof.relation = 'instanceOf' and instanceof.id = itemof.dependsonid and itemof.relation = 'itemOf' and heldby.relation = 'heldBy' and itemof.id = heldby.id and heldby.dependsonid in (select id from lddb__identifiers where iri in ('https://libris.kb.se/library/Ssb', 'https://libris.kb.se/library/SsbE'))) to stdout;" | sed 's/\\\\/\\/g' | gzip > stg-lddb-works-heldbyssb.jsonl.gz
*/
import java.util.zip.GZIPInputStream

import static whelk.util.Jackson.mapper

var DATADIR = System.properties['nowhelk.datadir'] ?: '/var/tmp/kb'

var name = System.properties['nowhelk.data'] ?: 'stg-lddb-bib-heldbyssb'
var worksName = System.properties['nowhelk.works'] ?: 'stg-lddb-works-heldbyssb'

var inDataFileName = "$DATADIR/${name}.jsonl.gz"
var outDataFileName = "$DATADIR/${name}-NORMALIZED.jsonl"
var workDataFileName = "$DATADIR/${worksName}.jsonl.gz"

System.err.println inDataFileName
System.err.println outDataFileName
System.err.println workDataFileName

Closure normalizeTypes = script('algorithm.groovy')

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
}
