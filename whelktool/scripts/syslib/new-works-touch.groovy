// MUST be run with --allow-loud

BATCH_SIZE = 5000
File allInstancesToResave = new File(scriptDir, "new-work-instances-touch-ids.txt")
String touchedIds = System.getProperty("touched-ids-file", '')
File touchedIdsFile = new File(touchedIds)
def allInstances = allInstancesToResave.readLines() as Set
def touched = touchedIdsFile.readLines() as Set
def ids = (allInstances - touched).take(BATCH_SIZE)

selectByIds( ids ) { instance ->
    instance.scheduleSave(loud: true)
    touchedIdsFile.withWriterAppend { writer ->
        writer.append(instance.doc.shortId + "\n")
    }
}
