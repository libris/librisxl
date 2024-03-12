// MUST be run with --allow-loud

BATCH_SIZE = 5000
File allWorksToResave = new File(scriptDir, "new-works-touch-ids.txt")
String touchedIds = System.getProperty("touched-ids-file", '')
File touchedIdsFile = new File(touchedIds)
def allWorks = allWorksToResave.readLines() as Set
def touched = touchedIdsFile.readLines() as Set
def ids = (allWorks - touched).take(BATCH_SIZE)

selectByIds( ids ) { work ->
    work.scheduleSave(loud: false)
    touchedIdsFile.withWriterAppend { writer ->
        writer.append(work.doc.shortId + "\n")
    }
}
