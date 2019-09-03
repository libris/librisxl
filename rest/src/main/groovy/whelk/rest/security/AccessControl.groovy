package whelk.rest.security

import groovy.util.logging.Log4j2 as Log
import whelk.Document
import whelk.JsonLd
import whelk.exception.ModelValidationException

@Log
class AccessControl {
    static final XLREG_KEY = 'registrant'
    static final GLOBALREG_KEY = 'global_registrant'
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

            // allow global registrant to correct holdings with missing sigel
            if (newDocSigel && !oldDocSigel && hasGlobalRegistrantPermission(userPrivileges)) {
                return true
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

            return hasGlobalRegistrantPermission(userPrivileges) || hasPermissionForSigel(sigel, userPrivileges)
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

    private boolean hasGlobalRegistrantPermission(Map userPrivileges) {
        return activeSigelPermissions(userPrivileges)
                .map({p -> p[GLOBALREG_KEY] == true})
                .orElse(false)
    }

    boolean isValidActiveSigel(Map userPrivileges) {
        return activeSigelPermissions(userPrivileges).isPresent()
    }

    private Optional<Map> activeSigelPermissions(Map userPrivileges) {
        String activeSigel = userPrivileges.get('active_sigel')
        Map permissions = null
        if (activeSigel) {
            permissions = userPrivileges.permissions.find { permission ->
                return permission.code == activeSigel
            }
        }
        return Optional.ofNullable(permissions)
    }
}
