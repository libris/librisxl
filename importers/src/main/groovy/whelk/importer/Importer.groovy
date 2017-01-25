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
        String version = whelk.version
        if (version ==~ /\d+\.\d+\.\d+-\w+-\w+/) {
            log.debug("Whelk version contains commit marker, using only tag version info.")
            version = version.split("-")[0]
        }
        log.info("Preparing import. Noting version ${version} in system settings.")
        def settings = whelk.storage.loadSettings("system")
        settings.put("version", version)
        whelk.storage.saveSettings("system", settings)

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
