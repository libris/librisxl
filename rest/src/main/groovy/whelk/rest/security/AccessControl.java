package whelk.rest.security

import groovy.util.logging.Log4j2 as Log
import whelk.Document
import whelk.JsonLd
import whelk.datatool.bulkchange.BulkAccessControl
import whelk.exception.ModelValidationException
import whelk.util.LegacyIntegrationTools

import static whelk.datatool.bulkchange.BulkJobDocument.JOB_TYPE

@Log
class AccessControl {
    static final XLREG_KEY = 'registrant'
    static final GLOBALREG_KEY = 'global_registrant'
    static final KAT_KEY = 'cataloger'


    static boolean checkDocumentToPost(Document newDoc, Map userPrivileges, JsonLd jsonld) {
        return checkDocument(newDoc, userPrivileges, jsonld)
    }

    // FIXME Mocking in CrudSpec breaks if this is made static
    boolean checkDocumentToPut(Document newDoc, Document oldDoc,
                                      Map userPrivileges, JsonLd jsonld) {
        if (oldDoc.isHolding(jsonld)) {
            def newDocSigel = newDoc.getHeldBySigel()
            def oldDocSigel = oldDoc.getHeldBySigel()

            if (!newDoc.isHolding(jsonld)) {
                // we don't allow changing from holding to non-holding
                return false
            }

            // we bail out early if sigel is missing in the new doc
            if (!newDocSigel) {
                throw new ModelValidationException('Missing sigel in document.')
            }

            // allow registrant to correct holdings with missing sigel
            if (newDocSigel && !oldDocSigel) {
                return hasGlobalRegistrantPermission(userPrivileges) ||
                        hasPermissionForSigel(newDocSigel, userPrivileges)
            }

            if (!(newDocSigel == oldDocSigel)) {
                log.warn("Trying to update content with an another sigel, " +
                        "denying request.")
                return false
            }

        }
        else if (oldDoc.getThingType() == JOB_TYPE) {
            BulkAccessControl.verify(oldDoc, newDoc)
        }

        return checkDocument(newDoc, userPrivileges, jsonld) &&
                checkDocument(oldDoc, userPrivileges, jsonld)
    }

    // FIXME Mocking in CrudSpec breaks if this is made static
    boolean checkDocumentToDelete(Document oldDoc, Map userPrivileges, JsonLd jsonld) {
        return checkDocument(oldDoc, userPrivileges, jsonld)
    }

    private static boolean checkDocument(Document document, Map userPrivileges, JsonLd jsonld) {
        if (!isValidActiveSigel(userPrivileges)) {
            return false
        }

        if (document.isHolding(jsonld)) {
            String sigel = document.getHeldBySigel()
            if (!sigel) {
                throw new ModelValidationException('Missing sigel in document.')
            }

            return hasGlobalRegistrantPermission(userPrivileges) || hasPermissionForSigel(sigel, userPrivileges)
        }
        else if (document.getThingType() == 'ShelfMarkSequence') {
            String ownedBySigel = LegacyIntegrationTools.uriToLegacySigel(document.getDescriptionCreator())
            return hasGlobalRegistrantPermission(userPrivileges) || hasPermissionForSigel(ownedBySigel, userPrivileges)
        }
        else if (document.getThingType() == JOB_TYPE) {
            // TODO step 1, new specific sigel instead
            // TODO step 2, configure as permission on user instead (in libris login)
            return hasPermissionForSigel("SEK", userPrivileges)
        }
        else if (document.isInReadOnlyDataset()) {
            return false
        } else {
            return hasCatalogingPermission(userPrivileges)
        }
    }

    private static boolean hasPermissionForSigel(String sigel, Map userPrivileges) {
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

    private static boolean hasCatalogingPermission(Map userPrivileges) {
        return userPrivileges.permissions.any { item ->
            item.get(KAT_KEY)
        }
    }

    private static boolean hasGlobalRegistrantPermission(Map userPrivileges) {
        return activeSigelPermissions(userPrivileges)
                .map({p -> p[GLOBALREG_KEY] == true})
                .orElse(false)
    }

    private static boolean isValidActiveSigel(Map userPrivileges) {
        return activeSigelPermissions(userPrivileges).isPresent()
    }

    private static Optional<Map> activeSigelPermissions(Map userPrivileges) {
        String activeSigel = userPrivileges.get('active_sigel')
        Map permissions = null
        if (activeSigel) {
            permissions = userPrivileges.permissions.find { permission ->
                return permission.code == activeSigel
            } as Map
        }
        return Optional.ofNullable(permissions)
    }
}
