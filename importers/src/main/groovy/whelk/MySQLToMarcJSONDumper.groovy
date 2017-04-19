package whelk

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j as Log

import java.nio.charset.Charset
import java.nio.file.Files
import java.nio.file.Paths

import org.codehaus.jackson.map.ObjectMapper

import whelk.importer.MySQLLoader

@Log
@CompileStatic
class MySQLToMarcJSONDumper implements MySQLLoader.LoadHandler {

    BufferedWriter dumpWriter
    ObjectMapper mapper
    def startTime

    MySQLToMarcJSONDumper(String dumpFileName) {
        dumpWriter = dumpFileName ?
                Files.newBufferedWriter(Paths.get(dumpFileName), Charset.forName("UTF-8"))
                : new BufferedWriter(System.out.newWriter())

        mapper = new ObjectMapper()
        startTime = System.currentTimeMillis()
    }

    @Override
    void handle(List<List<VCopyDataRow>> batch) {
        batch.each { rows ->
            dumpWriter.writeLine(
                    mapper.writeValueAsString(
                            VCopyToWhelkConverter.getMarcDocMap(rows[0].data)))
        }
    }
}
