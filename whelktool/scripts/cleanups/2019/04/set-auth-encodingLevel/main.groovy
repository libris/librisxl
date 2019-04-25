/*
 * This sets encodingLevel to marc:CompleteAuthorityRecord for all remaining auth
 * records with no property encodingLevel, or property with non-valid structure or value.
 *
 * See LXL-2056 for more info.
 *
 */

ENCODING_LEVEL = "encodingLevel"
VALID_AUTH_LEVELS = ["marc:CompleteAuthorityRecord", "marc:IncompleteAuthorityRecord"]
PrintWriter scheduledToSetEncodingLevel = getReportWriter("scheduled-to-set-encodingLevel")


selectByCollection('auth') { data ->

    def (record, authdata) = data.graph

    if (!record) return

    if (!record.encodingLevel ||
            (record.encodingLevel instanceof Map && record.encodingLevel[ID]) ||
            (!VALID_AUTH_LEVELS.contains(record.encodingLevel))) {
        record[ENCODING_LEVEL] = VALID_AUTH_LEVELS[0]
        scheduledToSetEncodingLevel.println("${record[ID]}")
        data.scheduleSave()
    }
}