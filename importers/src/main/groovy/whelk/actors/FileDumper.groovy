package whelk.actors


import groovy.util.logging.Slf4j
import whelk.Document
import whelk.util.VCopyToWhelkConverter
import whelk.component.PostgreSQLComponent
import whelk.converter.marc.MarcFrameConverter
import whelk.filter.LinkFinder
import whelk.importer.MySQLLoader
import whelk.util.ThreadPool

import java.nio.charset.Charset
import java.nio.file.Files
import java.nio.file.Paths

/**
 * Created by theodortolstoy on 2017-01-24.
 */
@Slf4j
class FileDumper implements MySQLLoader.LoadHandler {

    BufferedWriter mainTableWriter
    BufferedWriter identifiersWriter
    BufferedWriter dependenciesWriter
    ThreadPool threadPool

    /*
    In order to guard against the possibility that the conversion process may (one day?) no longer be reentrant,
    give each thread (slot) a converter of its' own.
     */
    MarcFrameConverter[] converterPool

    /*
    The PostgreSQLComponent is already otherwise required to be thread safe.
     */
    PostgreSQLComponent postgreSQLComponent

    FileDumper() {
        throw new Error("Groovy might let you call implicit default constructors, I will not.")
    }

    FileDumper(String exportFileName, PostgreSQLComponent postgres) {

        final int THREAD_COUNT = 4 * Runtime.getRuntime().availableProcessors()

        postgreSQLComponent = postgres
        mainTableWriter = Files.newBufferedWriter(Paths.get(exportFileName), Charset.forName("UTF-8"))
        identifiersWriter = Files.newBufferedWriter(Paths.get(exportFileName + "_identifiers"), Charset.forName("UTF-8"))
        dependenciesWriter = Files.newBufferedWriter(Paths.get(exportFileName + "_dependencies"), Charset.forName("UTF-8"))
        threadPool = new ThreadPool(THREAD_COUNT)
        converterPool = new MarcFrameConverter[THREAD_COUNT]
        for (int i = 0; i < THREAD_COUNT; ++i) {
            LinkFinder lf = new LinkFinder(postgreSQLComponent)
            converterPool[i] = new MarcFrameConverter(lf)
        }
    }

    public void close() {
        threadPool.joinAll()
        mainTableWriter.close()
        identifiersWriter.close()
        dependenciesWriter.close()
    }

    public void handle(List<List<VCopyToWhelkConverter.VCopyDataRow>> batch) {
        threadPool.executeOnThread(batch, { _batch, threadIndex ->
            List<Map> writeBatch = []

            for (List<VCopyToWhelkConverter.VCopyDataRow> rowList in _batch) {
                Map recordMap = null
                try {
                    recordMap = VCopyToWhelkConverter.convert(rowList, converterPool[threadIndex])
                }
                catch (Throwable e) {
                    log.error("Failed converting document with id: " + rowList.last().bib_id, e)
                }
                if (recordMap != null) {
                    List<String> depencyIDs = postgreSQLComponent.calculateDependenciesSystemIDs(recordMap.document)
                    recordMap["dependencies"] = depencyIDs
                    writeBatch.add(recordMap)
                }
            }

            append(writeBatch)
        })
    }

    private synchronized void append(List<Map> recordMaps) {
        for (Map recordMap in recordMaps) {

            if (recordMap) {

                Document doc = recordMap.document
                String coll = recordMap.collection
                String mainRecordId = doc.getCompleteId()
                String mainThingId = doc.getThingIdentifiers()[0]

                final String delimiterString = '\t'
                final String nullString = "\\N"

                mainTableWriter.write("${doc.shortId}\t" +
                        "${doc.dataAsString.replace("\\", "\\\\").replace(delimiterString, "\\" + delimiterString)}\t" +
                        "${coll.replace("\\", "\\\\").replace(delimiterString, "\\" + delimiterString)}\t" +
                        "${"vcopy"}\t" +
                        "${nullString}\t" +
                        "${recordMap.checksum.replace("\\", "\\\\").replace(delimiterString, "\\" + delimiterString)}\t" +
                        "${doc.created}\n")

                for (String identifier : doc.getRecordIdentifiers()) {
                    if (identifier == mainRecordId)
                        identifiersWriter.write("${doc.shortId}\t${identifier}\t0\ttrue\n")
                    else
                        identifiersWriter.write("${doc.shortId}\t${identifier}\t0\tfalse\n")
                }
                for (String identifier : doc.getThingIdentifiers()) {
                    if (identifier == mainThingId)
                        identifiersWriter.write("${doc.shortId}\t${identifier}\t1\ttrue\n")
                    else
                        identifiersWriter.write("${doc.shortId}\t${identifier}\t1\tfalse\n")
                }

                for (String dependency : recordMap.dependencies) {
                    dependenciesWriter.write("${doc.shortId}\t${dependency}\n")
                }
            }
        }
    }
}
