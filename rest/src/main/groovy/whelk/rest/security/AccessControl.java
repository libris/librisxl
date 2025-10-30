package whelk.rest.security;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import whelk.Document;
import whelk.JsonLd;
import whelk.datatool.bulkchange.BulkAccessControl;
import whelk.exception.ModelValidationException;
import whelk.util.LegacyIntegrationTools;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static whelk.datatool.bulkchange.BulkJobDocument.JOB_TYPE;

public class AccessControl {
    private static final Logger log = LogManager.getLogger(AccessControl.class);

    static final String XLREG_KEY = "registrant";
    static final String GLOBALREG_KEY = "global_registrant";
    static final String KAT_KEY = "cataloger";

    public static boolean checkDocumentToPost(Document newDoc, Map userPrivileges, JsonLd jsonld) {
        return checkDocument(newDoc, userPrivileges, jsonld);
    }

    // FIXME Mocking in CrudSpec breaks if this is made static
    public boolean checkDocumentToPut(Document newDoc, Document oldDoc,
                                      Map userPrivileges, JsonLd jsonld) {
        if (oldDoc.isHolding(jsonld)) {
            String newDocSigel = newDoc.getHeldBySigel();
            String oldDocSigel = oldDoc.getHeldBySigel();

            if (!newDoc.isHolding(jsonld)) {
                // we don't allow changing from holding to non-holding
                return false;
            }

            // we bail out early if sigel is missing in the new doc
            if (newDocSigel == null) {
                throw new ModelValidationException("Missing sigel in document.");
            }

            // allow registrant to correct holdings with missing sigel
            if (newDocSigel != null && oldDocSigel == null) {
                return hasGlobalRegistrantPermission(userPrivileges) ||
                        hasPermissionForSigel(newDocSigel, userPrivileges);
            }

            if (!newDocSigel.equals(oldDocSigel)) {
                log.warn("Trying to update content with an another sigel, denying request.");
                return false;
            }
        }
        else if (JOB_TYPE.equals(oldDoc.getThingType())) {
            BulkAccessControl.verify(oldDoc, newDoc);
        }

        return checkDocument(newDoc, userPrivileges, jsonld) &&
                checkDocument(oldDoc, userPrivileges, jsonld);
    }

    // FIXME Mocking in CrudSpec breaks if this is made static
    public boolean checkDocumentToDelete(Document oldDoc, Map userPrivileges, JsonLd jsonld) {
        return checkDocument(oldDoc, userPrivileges, jsonld);
    }

    private static boolean checkDocument(Document document, Map userPrivileges, JsonLd jsonld) {
        if (!isValidActiveSigel(userPrivileges)) {
            return false;
        }

        if (document.isHolding(jsonld)) {
            String sigel = document.getHeldBySigel();
            if (sigel == null) {
                throw new ModelValidationException("Missing sigel in document.");
            }

            return hasGlobalRegistrantPermission(userPrivileges) || hasPermissionForSigel(sigel, userPrivileges);
        }
        else if (document.getThingType().equals("ShelfMarkSequence")) {
            String ownedBySigel = LegacyIntegrationTools.uriToLegacySigel(document.getDescriptionCreator());
            return hasGlobalRegistrantPermission(userPrivileges) || hasPermissionForSigel(ownedBySigel, userPrivileges);
        }
        else if ((document.getThingType().equals(JOB_TYPE))) {
            // TODO step 1, new specific sigel instead
            // TODO step 2, configure as permission on user instead (in libris login)
            return hasPermissionForSigel("SEK", userPrivileges);
        }
        else if (document.isInReadOnlyDataset()) {
            return false;
        } else {
            return hasCatalogingPermission(userPrivileges);
        }
    }

    private static boolean hasPermissionForSigel(String sigel, Map userPrivileges) {
        boolean result = false;

        // redundant, but we want to safeguard against future mishaps
        if (sigel == null) {
            throw new ModelValidationException("Missing sigel in document.");
        }

        List<Map<String, Object>> permissions = (List<Map<String, Object>>) userPrivileges.get("permissions");
        for (Map<String, Object> item : permissions) {
            if (item.get("code").equals(sigel)) {
                Boolean xlregPermission = (Boolean) item.get(XLREG_KEY);
                Boolean katPermission = (Boolean) item.get(KAT_KEY);

                if (Boolean.TRUE.equals(katPermission) || Boolean.TRUE.equals(xlregPermission)) {
                    result = true;
                }
            }
        }

        return result;
    }

    private static boolean hasCatalogingPermission(Map userPrivileges) {
        List<Map<String, Object>> permissions = (List<Map<String, Object>>) userPrivileges.get("permissions");
        for (Map<String, Object> item : permissions) {
            Boolean katPermission = (Boolean) item.get(KAT_KEY);
            if (Boolean.TRUE.equals(katPermission)) {
                return true;
            }
        }
        return false;
    }

    private static boolean hasGlobalRegistrantPermission(Map userPrivileges) {
        return activeSigelPermissions(userPrivileges)
                .map(p -> Boolean.TRUE.equals(p.get(GLOBALREG_KEY)))
                .orElse(false);
    }

    private static boolean isValidActiveSigel(Map userPrivileges) {
        return activeSigelPermissions(userPrivileges).isPresent();
    }

    private static Optional<Map> activeSigelPermissions(Map userPrivileges) {
        String activeSigel = (String) userPrivileges.get("active_sigel");
        Map permissions = null;
        if (activeSigel != null) {
            List<Map<String, Object>> permissionsList = (List<Map<String, Object>>) userPrivileges.get("permissions");
            for (Map<String, Object> permission : permissionsList) {
                if (activeSigel.equals(permission.get("code"))) {
                    permissions = permission;
                    break;
                }
            }
        }
        return Optional.ofNullable(permissions);
    }
}
