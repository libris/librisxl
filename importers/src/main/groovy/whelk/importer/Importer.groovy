package whelk.importer

import groovy.util.logging.Slf4j as Log
import whelk.Whelk

/**
 * Created by markus on 2016-01-13.
 */
@Log
abstract class Importer {

    Whelk whelk

    final void run(String collection) {
        log.info("Preparing import of collection ${collection}.")
        doImport(collection)
    }

    abstract void doImport(String collection)

    abstract ImportResult doImport(String collection, String sourceSystem, Date from)
}

class BrokenRecordException extends Exception {
    String brokenId

    BrokenRecordException(String identifier) {
        super("Record ${identifier} has broken metadata")
        brokenId = identifier
    }
}
