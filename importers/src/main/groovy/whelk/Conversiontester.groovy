package whelk

import se.kb.libris.util.marc.io.Iso2709Deserializer
import whelk.converter.marc.JsonLD2MarcConverter
import whelk.converter.marc.MarcFrameConverter
import whelk.importer.MySQLLoader
import whelk.util.ThreadPool

import java.nio.charset.Charset
import java.nio.file.Files
import java.nio.file.Paths

class Conversiontester implements MySQLLoader.LoadHandler {

    private ThreadPool threadPool
    private MarcFrameConverter[] toJsonConverterPool
    private JsonLD2MarcConverter[] toMarcConverterPool
    private BufferedWriter outputWriter

    public Conversiontester() {
        final int THREAD_COUNT = 4*Runtime.getRuntime().availableProcessors()

        outputWriter = Files.newBufferedWriter(Paths.get("conversion_errors.log"), Charset.forName("UTF-8"))
        threadPool = new ThreadPool(THREAD_COUNT)
        toJsonConverterPool = new MarcFrameConverter[THREAD_COUNT]
        toMarcConverterPool = new JsonLD2MarcConverter[THREAD_COUNT]
        for (int i = 0; i < THREAD_COUNT; ++i) {
            toJsonConverterPool[i] = new MarcFrameConverter()
            toMarcConverterPool[i] = new JsonLD2MarcConverter()
        }
    }

    public void handle(List<List<VCopyDataRow>> batch) {
        threadPool.executeOnThread( batch, { _batch, threadIndex ->

            for ( List<VCopyDataRow> rowList in _batch ) {

                String voyagerId = getVoyagerId(rowList)

                Map recordMap

                try {
                    recordMap = VCopyToWhelkConverter.convert(rowList, toJsonConverterPool[threadIndex])
                } catch (Throwable throwable) {
                    logConversionFailure(voyagerId, rowList.last().data, "None, failed at initial MARC->JSON conversion", throwable)
                    continue
                }

                Document xlDocument = recordMap.document

                try {
                    toMarcConverterPool[threadIndex].convert(xlDocument.data, xlDocument.getShortId())
                } catch (Throwable throwable) {
                    logConversionFailure(voyagerId, rowList.last().data, xlDocument.getDataAsString(), throwable)
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

        outputWriter.writeLine(errorString + "\n---------------\n")
    }

    private getVoyagerId(List<VCopyDataRow> rowList) {
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
            outputWriter.writeLine("Fatal: Could not determine Voyager ID of incoming Voyager post. Aborting.")
            outputWriter.close()
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
        outputWriter.close()
    }
}
