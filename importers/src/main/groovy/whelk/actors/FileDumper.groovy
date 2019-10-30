package whelk.actors


import groovy.util.logging.Log4j2 as Log
import whelk.Document
import whelk.Whelk
import whelk.util.VCopyToWhelkConverter
import whelk.component.PostgreSQLComponent
import whelk.converter.marc.MarcFrameConverter
import whelk.importer.MySQLLoader
import whelk.util.ThreadPool

import java.nio.charset.Charset
import java.nio.file.Files
import java.nio.file.Paths
import java.sql.Timestamp
import java.time.Instant
import java.time.ZoneId
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter

@Log
@Deprecated
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

    Whelk whelk

    FileDumper() {
        throw new Error("Groovy might let you call implicit default constructors, I will not.")
    }

    FileDumper(String exportFileName, PostgreSQLComponent postgres) {

        final int THREAD_COUNT = getThreadCount()

        whelk = new Whelk(postgres)
        whelk.loadCoreData()
        mainTableWriter = Files.newBufferedWriter(Paths.get(exportFileName), Charset.forName("UTF-8"))
        identifiersWriter = Files.newBufferedWriter(Paths.get(exportFileName + "_identifiers"), Charset.forName("UTF-8"))
        dependenciesWriter = Files.newBufferedWriter(Paths.get(exportFileName + "_dependencies"), Charset.forName("UTF-8"))
        threadPool = new ThreadPool(THREAD_COUNT)
        converterPool = new MarcFrameConverter[THREAD_COUNT]
        for (int i = 0; i < THREAD_COUNT; ++i) {
            converterPool[i] = whelk.getMarcFrameConverter()
        }
    }

    private int getThreadCount() {
        int procs = Runtime.getRuntime().availableProcessors()
        if (procs > 1) {
            return procs - 1
        } else {
            return procs
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
                    List<String[]> externalDependencies = whelk.storage.calculateDependenciesSystemIDs(recordMap.document)
                    recordMap["dependencies"] = externalDependencies

                    String modifiedString = recordMap.document.getModified()
                    if (modifiedString == null)
                        modifiedString = DateTimeFormatter.ISO_OFFSET_DATE_TIME.format( ZonedDateTime.now(ZoneId.systemDefault()) )

                    ZonedDateTime modifiedZoned = ZonedDateTime.parse(modifiedString, DateTimeFormatter.ISO_OFFSET_DATE_TIME)
                    Date modified = Date.from(modifiedZoned.toInstant())
                    recordMap.document.setModified(modified)

                    recordMap.document.setGenerationProcess('https://id.kb.se/generator/marcframe')
                    recordMap.document.setGenerationDate(new Date())

                    boolean cacheAuthForever = true
                    converterPool[threadIndex].linkFinder.normalizeIdentifiers(recordMap.document, cacheAuthForever)
                    if (externalDependencies.size() > 0) {
                        List<String> dependencyIDs = []
                        for (String[] reference : externalDependencies) {
                            dependencyIDs.add(reference[1])
                        }
                        Tuple2<Timestamp, Timestamp> depMinMaxModified = whelk.storage.getMinMaxModified(dependencyIDs)

                        Instant min = ((Timestamp) depMinMaxModified.get(0)).toInstant()
                        Instant max = ((Timestamp) depMinMaxModified.get(1)).toInstant()
                        Instant modifiedInstant = modified.toInstant()

                        if (modifiedInstant.isBefore(min))
                            min = modifiedInstant
                        if (modifiedInstant.isAfter(max))
                            max = modifiedInstant

                        recordMap["depMinModified"] = DateTimeFormatter.ISO_OFFSET_DATE_TIME.format( ZonedDateTime.ofInstant(min, ZoneId.systemDefault()) )
                        recordMap["depMaxModified"] = DateTimeFormatter.ISO_OFFSET_DATE_TIME.format( ZonedDateTime.ofInstant(max, ZoneId.systemDefault()) )
                    }
                    else
                        recordMap["depMinModified"] = recordMap["depMaxModified"] = recordMap.document.getModified()
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
                        nullString + delimiterString + // changed by
                        "${recordMap.checksum.replace("\\", "\\\\").replace(delimiterString, "\\" + delimiterString)}\t" +
                        "${doc.created}\t" +
                        "${doc.modified}\t" +
                        "false\t" + // deleted
                        recordMap["depMinModified"] + delimiterString +
                        recordMap["depMaxModified"] + "\n"
                )

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

                for (String[] dependency : recordMap.dependencies) {
                    dependenciesWriter.write("${doc.shortId}\t${dependency[0]}\t${dependency[1]}\n")
                }
            }
        }
    }
}
