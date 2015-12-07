package whelk.util

import whelk.DateUtil
import whelk.IdGenerator

/**
 * Created by markus on 2015-12-07.
 */
class LegacyIntegrationTools {

    static final long LEGACY_IDENTIFIER_BASETIME = DateUtil.parseDate("2001-01-01").getTime()
    static Map DATASET_ID_SEPARATOR = ["auth": "1", "bib": "2", "hold": 3]


    static String generateId(String originalIdentifier) {
        String[] parts = originalIdentifier.split("/")
        long numericId = new Long("" +
                (LEGACY_IDENTIFIER_BASETIME + Integer.parseInt(parts.last())) +
                DATASET_ID_SEPARATOR[parts[1]]).longValue()
        return IdGenerator.generate(numericId, originalIdentifier, 12)
    }


}
