package whelk.plugin.libris.security
import groovy.util.logging.Slf4j as Log

import whelk.Document
import whelk.plugin.BasicPlugin

@Log
class AccessControl extends BasicPlugin {

    boolean checkDocument(Document newdoc, Document olddoc, Map userPrivileges) {
        if (newdoc?.dataset == "hold") {
            def sigel = newdoc.dataAsMap.about.heldBy.notation
            log.debug("User tries to change a holding for sigel ${sigel}.")

            def privs = userPrivileges.authorization.find { it.sigel == sigel }
            log.trace("User has these privs for ${sigel}: $privs")
            //if (!privs?.reg && !privs?.kat) {
            if (!privs?.xlreg) {
                log.debug("User does not have sufficient privileges.")
                return false
            }

            def currentSigel = olddoc?.dataAsMap?.about?.heldBy?.notation
            if (currentSigel) {
                log.trace("Checking sigel privs for existing document.")
                privs = userPrivileges.authorization.find { it.sigel == currentSigel }
                log.trace("User has these privs for current sigel ${sigel}: $privs")
                //if (!privs?.reg && !privs?.kat) {
                if (!privs?.xlreg) {
                    log.debug("User does NOT have enough privileges.")
                    return false
                }
            }
        } else {
            log.info("Datasets 'bib' and 'auth' are not editable right now.")
            return false
        }

        if (newdoc) {
            newdoc.entry.lastChangeBy = userPrivileges.username
        }
        log.debug("User is authorized to make the change.")
        return true
    }
}
