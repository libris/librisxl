// MUST be run with --allow-loud

BATCH_SIZE = 2000
File allWorksToResave = new File(scriptDir, "new-works-touch-ids.txt")
String touchedIds = System.getProperty("touched-ids-file", '')
File touchedIdsFile = new File(touchedIds)
List touched = touchedIdsFile.readLines()
def count = 0

selectByIds( allWorksToResave.readLines() ) { work ->
    if (touched.contains(work.doc.shortId) || count >= BATCH_SIZE) {
        return
    } else {
        work.scheduleSave(loud: true)
        count++
        touchedIdsFile.withWriterAppend { writer ->
            writer.append(work.doc.shortId + "\n")
        }
    }
}
