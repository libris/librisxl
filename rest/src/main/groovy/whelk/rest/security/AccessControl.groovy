package whelk.rest.security
import groovy.util.logging.Slf4j as Log

import whelk.Document
import whelk.JsonLd
import whelk.exception.ModelValidationException


@Log
class AccessControl {
    private static final XLREG_KEY = 'xlreg'
    private static final KAT_KEY = 'kat'

    boolean checkDocumentToPost(Document newDoc, Map userPrivileges) {
        return checkDocument(newDoc, userPrivileges)
    }

    boolean checkDocumentToPut(Document newDoc, Document oldDoc,
                               Map userPrivileges) {
        def newDocSigel = newDoc.getSigel()
        def oldDocSigel = oldDoc.getSigel()

		// we bail out early if sigel is missing in the new doc
		if (!newDocSigel) {
			throw new ModelValidationException('Missing sigel in document.')
		}

        if (!(newDocSigel == oldDocSigel)) {
            log.warn("Trying to update content with an another sigel, " +
                     "denying request.")
            return false
        }

        return checkDocument(newDoc, userPrivileges) &&
                    checkDocument(oldDoc, userPrivileges)
    }

    boolean checkDocumentToDelete(Document oldDoc, Map userPrivileges) {
        return checkDocument(oldDoc, userPrivileges)
    }

    boolean checkDocument(Document document, Map userPrivileges) {
        String id = document.getShortId()
        String sigel = document.getSigel()

        boolean result = false

        log.debug("Checking permissions for document ${id}")

        if (!sigel){
            log.warn("No sigel found in document ${id}, denying request.")
            throw new ModelValidationException('Missing sigel in document.')
        }

        userPrivileges.authorization.each { item ->
            if (item.get("sigel") == sigel) {
                boolean xlreg_permission = item.get(XLREG_KEY)
                boolean kat_permission = item.get(KAT_KEY)

                if (xlreg_permission) {
                    result = document.isHolding()
                } else if (kat_permission) {
                    result = true
                }
            }
        }

        return result
    }

}
