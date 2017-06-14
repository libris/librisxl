package whelk

import java.text.SimpleDateFormat

class DateUtil {

    static Date parseDate(String date) {
        return parseDateTime(date + "T00:00:00Z")
    }

    static Date parseDateTime(String date) {
        return new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssX").parse(date)
    }

}
