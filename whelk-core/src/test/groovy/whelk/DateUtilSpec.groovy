package whelk

import spock.lang.Specification

class DateUtilSpec extends Specification {

    def "should parse dates"() {
        expect:
        DateUtil.parseDate("1970-01-01").time == 0
        DateUtil.parseDateTime("1970-01-01T00:00:00Z").time == 0
        DateUtil.parseDateTime("1970-01-01T23:59:00Z").time == (23 * 60 * 60 * 1000) + (59 * 60 * 1000)
    }

}
