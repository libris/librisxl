package whelk

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

                Map recordMap

                try {
                    recordMap = VCopyToWhelkConverter.convert(rowList, toJsonConverterPool[threadIndex])
                } catch (Throwable throwable) {
                    // Log marc->json error
                    String marcString = MySQLLoader.normalizeString(new String(rowList.last().data, "UTF-8"))
                    outputErrorLog(marcString, throwable)
                    continue
                }

                Document xlDocument = recordMap.document

                try {
                    Map reconvertedMarcData = toMarcConverterPool[threadIndex].convert(xlDocument.data, xlDocument.getShortId())
                } catch (Throwable throwable) {
                    // Log json->marc error
                    outputErrorLog(xlDocument.getDataAsString(), throwable)
                }
            }
        })
    }

    private synchronized outputErrorLog(String sourceData, Throwable throwable) {

        String errorString = "Conversion failed. The data on which conversion was attempted:\n" +
                sourceData + "\n\n Failed on: "  + throwable.toString() + ". Callstack was:\n" +
                getHumanReadableCallStack(throwable)

        outputWriter.writeLine(errorString + "---------------\n")
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
