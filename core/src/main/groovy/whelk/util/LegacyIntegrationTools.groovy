package whelk.util

import whelk.DateUtil
import whelk.IdGenerator

class LegacyIntegrationTools {

    static final long LEGACY_IDENTIFIER_BASETIME = DateUtil.parseDate("2001-01-01").getTime()
    static Map<String, String> DATASET_ID_SEPARATOR = ["auth": "1", "bib": "2", "hold": "3"]

    static String generateId(String originalIdentifier) {
        String[] parts = originalIdentifier.split("/")
        long numericId = LEGACY_IDENTIFIER_BASETIME + Integer.parseInt(parts.last())
        String dataSetSuffix = "4"
        if (DATASET_ID_SEPARATOR.containsKey(parts[1])) {
            dataSetSuffix = DATASET_ID_SEPARATOR[parts[1]]
        }
        return IdGenerator.generate(numericId, originalIdentifier, 11) + dataSetSuffix
    }

}
