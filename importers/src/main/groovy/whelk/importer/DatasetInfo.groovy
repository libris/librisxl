package whelk.importer

import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
import java.util.regex.Pattern

import groovy.transform.CompileStatic

import whelk.IdGenerator
import whelk.JsonLd

@CompileStatic
class DatasetInfo {
    static ID = JsonLd.ID_KEY

    String uri
    long createdMs
    String uriSpace
    Pattern uriRegexPattern

    DatasetInfo(Map data) {
        uri = data[ID]
        uriSpace = data['uriSpace']
        String uriRegexPatternStr = data['uriRegexPattern']
        if (uriRegexPatternStr != null) {
            uriRegexPattern = Pattern.compile(uriRegexPatternStr)
        }
        String created = (String) data['created']
        if (created != null) {
            createdMs = parseW3CDateTime(created)
        }
    }

    String mintPredictableRecordSlug(String givenId) {
        String slug = givenId
        long timestamp = createdMs + fauxOffset(slug)
        return IdGenerator.generate(timestamp, slug)
    }

    static long parseW3CDateTime(String dt) {
        return ZonedDateTime.parse(dt, DateTimeFormatter.ISO_OFFSET_DATE_TIME).toInstant().toEpochMilli()
    }

    static int fauxOffset(String s) {
        int n = 0
        for (int i = 0 ; i < s.size(); i++) {
            n += s[i].codePointAt(0) * ((i+1) ** 2)
        }
        return n
    }

}
