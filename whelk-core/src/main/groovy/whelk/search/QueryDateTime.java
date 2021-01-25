package whelk.search;

import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.temporal.TemporalAccessor;
import java.util.Arrays;
import java.util.stream.Collectors;

public class QueryDateTime {
    public enum Precision {
        YEAR(   "uuuu",                  "uuuu'||/y'"),
        MONTH(  "uuuu-MM",               "uuuu-MM'||/M'"),
        DAY(    "uuuu-MM-dd",            "uuuu-MM-dd'||/d'"),
        HOUR(   "uuuu-MM-dd'T'HH",       "uuuu-MM-dd'T'HH'||/H'"),
        MINUTE( "uuuu-MM-dd'T'HH:mm",    "uuuu-MM-dd'T'HH:mm'||/m'"),
        SECOND( "uuuu-MM-dd'T'HH:mm:ss", "uuuu-MM-dd'T'HH:mm:ss'||/s'"),
        WEEK(   "YYYY'-W'ww",            "YYYY'W'ww'1||/w'");

        String format;
        DateTimeFormatter parser;
        DateTimeFormatter elasticFormat;

        Precision(String format, String elasticFormat) {
            this.format = format;
            this.parser = DateTimeFormatter.ofPattern(format);
            this.elasticFormat = DateTimeFormatter.ofPattern(elasticFormat);
        }

        String format() {
            return format;
        }
    }

    TemporalAccessor time;
    Precision precision;

    public QueryDateTime(TemporalAccessor time, Precision precision) {
        this.time = time;
        this.precision = precision;
    }

    public String toElasticDateString() {
        return precision.elasticFormat.format(time);
    }

    static QueryDateTime parse(String s) throws DateTimeParseException {
        for (Precision f : Precision.values()) {
            try {
                return new QueryDateTime(f.parser.parse(s), f);
            } catch (DateTimeParseException ignored) {}
        }

        String msg = String.format("Could not parse date: %s. Valid formats: %s", s,
                Arrays.stream(Precision.values()).map(Precision::format).collect(Collectors.joining(", ")));

        throw new DateTimeParseException(msg , s, 0);
    }
}
