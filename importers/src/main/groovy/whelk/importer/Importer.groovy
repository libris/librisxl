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
        log.info("Preparing import. Noting version ${whelk.version} in system settings.")
        def settings = whelk.storage.loadSettings("system")
        settings.put("version", whelk.version)
        whelk.storage.saveSettings("system", settings)

        doImport(collection)
    }

    abstract void doImport(String collection)
}
