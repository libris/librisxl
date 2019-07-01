package whelk.rest.security

import groovy.util.logging.Log4j2 as Log
import whelk.Document
import whelk.JsonLd
import whelk.exception.ModelValidationException

@Log
class AccessControl {
    static final XLREG_KEY = 'registrant'
    static final KAT_KEY = 'cataloger'

    boolean checkDocumentToPost(Document newDoc, Map userPrivileges, JsonLd jsonld) {
        return checkDocument(newDoc, userPrivileges, jsonld)
    }

    boolean checkDocumentToPut(Document newDoc, Document oldDoc,
                               Map userPrivileges, JsonLd jsonld) {
		if (oldDoc.isHolding(jsonld)) {
			def newDocSigel = newDoc.getSigel()
			def oldDocSigel = oldDoc.getSigel()

			if (!newDoc.isHolding(jsonld)) {
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

		return checkDocument(newDoc, userPrivileges, jsonld) &&
					checkDocument(oldDoc, userPrivileges, jsonld)
    }

    boolean checkDocumentToDelete(Document oldDoc, Map userPrivileges, JsonLd jsonld) {
        return checkDocument(oldDoc, userPrivileges, jsonld)
    }

    boolean checkDocument(Document document, Map userPrivileges, JsonLd jsonld) {
        if (!isValidActiveSigel(userPrivileges)) {
            return false
        }

        if (document.isHolding(jsonld)) {
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

        userPrivileges.permissions.each { item ->
            if (item.get("code") == sigel) {
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
        return userPrivileges.permissions.any { item ->
            item.get(KAT_KEY)
        }
    }

    boolean isValidActiveSigel(Map userPrivileges) {
        String activeSigel = userPrivileges.get('active_sigel')
        if (activeSigel) {
            userPrivileges.permissions.any { permission ->
                return permission.code == activeSigel
            }
        } else {
            return false
        }
    }
}
