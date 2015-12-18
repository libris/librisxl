package whelk.util

import whelk.DateUtil
import whelk.IdGenerator

class LegacyIntegrationTools {

    static final Map<String,Long> BASETIMES = [
        "auth": DateUtil.parseDate("1980-01-01").getTime(),
        "bib": DateUtil.parseDate("1984-01-01").getTime(),
        "hold": DateUtil.parseDate("1988-01-01").getTime()
    ]

    static String generateId(String originalIdentifier) {
        String[] parts = originalIdentifier.split("/")
        long basetime = BASETIMES[parts[1]]
        long numericId = basetime + Integer.parseInt(parts.last())
        return IdGenerator.generate(numericId, originalIdentifier)
    }

}
