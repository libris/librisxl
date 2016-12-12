package whelk.rest.security
import groovy.util.logging.Slf4j as Log

import whelk.Document
import whelk.JsonLd
import whelk.exception.ModelValidationException


@Log
class AccessControl {
    boolean checkDocumentToPost(Document newDoc, Map userPrivileges) {
        return checkDocument(newDoc, userPrivileges)
    }

    boolean checkDocumentToPut(Document newDoc, Document oldDoc, Map userPrivileges) {
        boolean result = false
        def newDocSigel = newDoc.getSigel()
        def oldDocSigel = oldDoc.getSigel()

        if (!newDocSigel || !oldDocSigel){
            log.warn("No sigel found in document, denying request.")
            return false
        }

        if (!(newDocSigel == oldDocSigel)) {
            log.warn("Trying to update content with an another sigel, denying request.")
            return false
        }

        userPrivileges.authorization.each { item ->
            if (item.get("sigel") == newDocSigel) {
                if (item.get("xlreg")) {
                    result = (newDoc.isHolding() && oldDoc.isHolding())
                } else if (item.get("kat")) {
                    result = true
                }
            }
        }

        return result
    }

    boolean checkDocumentToDelete(Document oldDoc, Map userPrivileges) {
        return checkDocument(oldDoc, userPrivileges)
    }

    boolean checkDocument(Document document, Map userPrivileges) {
        boolean result = false
        def currentSigel = document.getSigel()

        if (!currentSigel){
            log.warn("No sigel found in document, denying request.")
            return result
        }

        userPrivileges.authorization.each { item ->
            if (item.get("sigel") == currentSigel) {
                if (item.get("xlreg")) {
                    result = document.isHolding()
                } else if (item.get("kat")) {
                    result = true
                }
            }
        }
        return result
    }

}
