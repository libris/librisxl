package whelk

import se.kb.libris.util.marc.io.Iso2709Deserializer
import whelk.component.PostgreSQLComponent
import whelk.converter.marc.JsonLD2MarcConverter
import whelk.converter.marc.MarcFrameConverter
import whelk.importer.MySQLLoader
import whelk.util.ThreadPool
import whelk.util.VCopyToWhelkConverter

import java.nio.charset.Charset
import java.nio.file.Files
import java.nio.file.Paths
import java.util.concurrent.atomic.AtomicLong

class Conversiontester implements MySQLLoader.LoadHandler {

    private ThreadPool threadPool
    private MarcFrameConverter[] toJsonConverterPool
    private JsonLD2MarcConverter[] toMarcConverterPool
    private BufferedWriter failureOutputWriter
    private BufferedWriter diffOutputWriter
    private final generateDiffFile
    private AtomicLong totalCount = new AtomicLong(0)
    private long failCount = 0
    private long diffCount = 0

    public Conversiontester(boolean generateDiffFile) {
        final int THREAD_COUNT = 4*Runtime.getRuntime().availableProcessors()

        failureOutputWriter = Files.newBufferedWriter(Paths.get("conversion_errors.log"), Charset.forName("UTF-8"))
        if (generateDiffFile)
            diffOutputWriter = Files.newBufferedWriter(Paths.get("conversion_diffs.log"), Charset.forName("UTF-8"))
        threadPool = new ThreadPool(THREAD_COUNT)
        toJsonConverterPool = new MarcFrameConverter[THREAD_COUNT]
        toMarcConverterPool = new JsonLD2MarcConverter[THREAD_COUNT]
        for (int i = 0; i < THREAD_COUNT; ++i) {
            toJsonConverterPool[i] = new MarcFrameConverter()
            toMarcConverterPool[i] = new JsonLD2MarcConverter()
        }
        this.generateDiffFile = generateDiffFile
    }

    public void handle(List<List<VCopyToWhelkConverter.VCopyDataRow>> batch) {
        threadPool.executeOnThread( batch, { _batch, threadIndex ->

            for ( List<VCopyToWhelkConverter.VCopyDataRow> rowList in _batch ) {
                totalCount.incrementAndGet()

                String voyagerId = getVoyagerId(rowList)

                Map conversionResultMap

                try {
                    conversionResultMap = VCopyToWhelkConverter.convert(rowList, toJsonConverterPool[threadIndex])
                } catch (Throwable throwable) {
                    logConversionFailure(voyagerId, rowList.last().data, "None, failed at initial MARC->JSON conversion", throwable)
                    continue
                }

                Document xlDocument = conversionResultMap.document
                if (xlDocument == null)
                    continue

                Map revertedMarcJsonMap
                try {
                    revertedMarcJsonMap = toMarcConverterPool[threadIndex].convert(xlDocument.data, xlDocument.getShortId())
                } catch (Throwable throwable) {
                    logConversionFailure(voyagerId, rowList.last().data, xlDocument.getDataAsString(), throwable)
                    continue
                }

                /*
                Conversions were ok. If the diff options was specified, diff the reverted and original marc records
                and log any discrepancies.
                 */
                if (generateDiffFile) {
                    Map originalMarcJsonMap = VCopyToWhelkConverter.getMarcDocMap(rowList.last().data)

                    // (Ab)use the whelk document checksum to check equality between marcJson documents
                    Document _original = new Document(originalMarcJsonMap)
                    Document _reverted = new Document(revertedMarcJsonMap)
                    if (_original.getChecksum() != _reverted.getChecksum())
                        logConversionDiff(voyagerId, originalMarcJsonMap, xlDocument.getDataAsString(), revertedMarcJsonMap)
                }
            }
        })
    }

    private synchronized logConversionFailure(String voyagerId, byte[] sourceMarc, String intermediateJson, Throwable throwable) {
        String marcString = Iso2709Deserializer.deserialize(MySQLLoader.normalizeString(new String(sourceMarc, "UTF-8")).getBytes()).toString()

        String errorString = voyagerId + " conversion failed.\n\nOriginal MARC was:\n" +
                marcString + "\n\nIntermediate JSON was:\n" + intermediateJson +
                "\n\nFailed on: "  + throwable.toString() + ". Callstack was:\n" +
                getHumanReadableCallStack(throwable)

        failureOutputWriter.writeLine(errorString + "\n---------------\n")
        ++failCount
    }

    private synchronized logConversionDiff(String voyagerId, Map originalMarcJsonMap, String intermediateJson, Map revertedMarcJsonMap) {
        diffOutputWriter.writeLine("Reverted MARC of " + voyagerId + " differed from original.\n\nOriginal MARC was:\n" +
                PostgreSQLComponent.mapper.writeValueAsString(originalMarcJsonMap) +
                "\n\nwas reverted to:\n" + PostgreSQLComponent.mapper.writeValueAsString(revertedMarcJsonMap) +
                "\n\nIntermediate JSONLD was:\n" + intermediateJson + "\n\n---------------\n")
        ++diffCount
    }

    private getVoyagerId(List<VCopyToWhelkConverter.VCopyDataRow> rowList) {
        // Each rowList pertains to _one_ voyager post. Get the ID of that post.
        String id = null
        switch (rowList.last().collection) {
            case "auth":
                id = "auth/" + rowList.last().auth_id
                break
            case "bib":
                id = "bib/" + rowList.last().bib_id
                break
            case "hold":
                id = "hold/" + rowList.last().mfhd_id
                break
        }

        if (id == null) { // Unless something is extremely wrong, this should be unreachable code.
            failureOutputWriter.writeLine("Fatal: Could not determine Voyager ID of incoming Voyager post. Aborting.")
            failureOutputWriter.close()
            System.exit(-1)
        }

        return id
    }

    private String getHumanReadableCallStack(Throwable e) {
        StringBuilder callStack = new StringBuilder("")
        for (StackTraceElement frame : e.getStackTrace())
            callStack.append(frame.toString() + "\n")
        return callStack
    }

    public void close() {
        threadPool.joinAll()
        failureOutputWriter.close()
        println("Conversion test completed.")
        println("" + totalCount.get() + " documents converted.")
        println("" + failCount + " documents failed.")
        if (generateDiffFile)
            println("" + diffCount + " documents diffed between original and reverted MARC.")
    }
}
