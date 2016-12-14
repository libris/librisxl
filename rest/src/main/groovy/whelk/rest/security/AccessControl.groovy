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
		if (oldDoc.isHolding()) {
			def newDocSigel = newDoc.getSigel()
			def oldDocSigel = oldDoc.getSigel()

			if (!newDoc.isHolding()) {
				// we don't allow changing from holding to non-holding
				return false
			}

			// we bail out early if sigel is missing in the new doc
			if (!newDocSigel) {
				throw new ModelValidationException('Missing sigel in document.')
			}

			if (!(newDocSigel == oldDocSigel)) {
				log.warn("Trying to update content with an another sigel, " +
						 "denying request.")
				return false
			}

		}

		return checkDocument(newDoc, userPrivileges) &&
					checkDocument(oldDoc, userPrivileges)
    }

    boolean checkDocumentToDelete(Document oldDoc, Map userPrivileges) {
        return checkDocument(oldDoc, userPrivileges)
    }

    boolean checkDocument(Document document, Map userPrivileges) {
        if (document.isHolding()) {
            String sigel = document.getSigel()
            if (!sigel) {
                throw new ModelValidationException('Missing sigel in document.')
            }

            return hasPermissionForSigel(sigel, userPrivileges)
        } else {
            return hasCatalogingPermission(userPrivileges)
        }
    }

    private boolean hasPermissionForSigel(String sigel, Map userPrivileges) {
        boolean result = false

        // redundant, but we want to safeguard against future mishaps
        if (!sigel) {
            throw new ModelValidationException('Missing sigel in document.')
        }

        userPrivileges.authorization.each { item ->
            if (item.get("sigel") == sigel) {
                boolean xlreg_permission = item.get(XLREG_KEY)
                boolean kat_permission = item.get(KAT_KEY)

                if (kat_permission || xlreg_permission) {
                    result = true
                }
            }
        }

        return result
    }

    private boolean hasCatalogingPermission(Map userPrivileges) {
        return userPrivileges.authorization.any { item ->
            item.get(KAT_KEY)
        }
    }
}
