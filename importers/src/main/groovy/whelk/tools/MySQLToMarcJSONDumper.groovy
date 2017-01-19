package whelk.tools

import groovy.transform.CompileStatic

import java.nio.charset.Charset
import java.nio.file.Files
import java.nio.file.Paths

import org.codehaus.jackson.map.ObjectMapper

import whelk.importer.MySQLLoader

@CompileStatic
class MySQLToMarcJSONDumper {

    static dump(String connectionUrl, String collection, String dumpFileName) {
        def mapper = new ObjectMapper()
        def loader = new MySQLLoader(connectionUrl, collection)
        def dumpWriter = dumpFileName ?
                Files.newBufferedWriter(Paths.get(dumpFileName), Charset.forName("UTF-8"))
                : new BufferedWriter(System.out.newWriter())

        def counter = 0
        def startTime = System.currentTimeMillis()

        try {
            loader.run { doc, specs ->

                dumpWriter.writeLine(mapper.writeValueAsString(doc))

                if (++counter % 1000 == 0) {
                    def elapsedSecs = (System.currentTimeMillis() - startTime) / 1000
                    if (elapsedSecs > 0) {
                        def docsPerSec = counter / elapsedSecs
                        System.err.println "Working. Currently $counter documents saved. Crunching $docsPerSec docs / s"
                    }
                }
            }
        } finally {
            dumpWriter.close()
        }

        def endSecs = (System.currentTimeMillis() - startTime) / 1000
        System.err.println "Done. Processed $counter documents in $endSecs seconds."
    }

}
