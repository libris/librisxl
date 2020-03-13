package whelk.search

import whelk.exception.InvalidQueryException

import java.time.ZoneId
import java.time.format.DateTimeParseException

import static whelk.search.ParameterPrefix.MATCHES
import static whelk.search.ParameterPrefix.MAX
import static whelk.search.ParameterPrefix.MAX_EX
import static whelk.search.ParameterPrefix.MIN
import static whelk.search.ParameterPrefix.MIN_EX
import static whelk.search.QueryDateTime.Precision.WEEK

class Ranges {
    String fieldName
    ZoneId timezone

    private List<Range> ranges = []

    private Ranges(String fieldName, ZoneId timezone = null) {
        this.fieldName = fieldName
        this.timezone = timezone
    }

    static Ranges nonDate(String fieldName) {
        new Ranges(fieldName)
    }

    static Ranges date(String fieldName, ZoneId timezone) {
        new Ranges(fieldName, timezone)
    }

    void add(ParameterPrefix operator, value) {
        switch (operator) {
            case MIN:
            case MIN_EX:
                Range openOrNew = (ranges.find { !it.min } ?: (ranges << new Range()).last())
                openOrNew.min = new EndPoint(operator, value)
                break;

            case MAX:
            case MAX_EX:
                Range openOrNew = (ranges.find { !it.max } ?: (ranges << new Range()).last())
                openOrNew.max = new EndPoint(operator, value)
                break;

            case MATCHES:
                ranges << new Range(new EndPoint(MIN, value), new EndPoint(MAX, value))
                break;
        }
    }

    boolean isDateField() {
        timezone != null
    }

    Map toQuery() {
        if (ranges.isEmpty()) {
            throw new IllegalStateException("no ranges")
        }

        try {
            ranges.size() > 1
                    ? ["bool": ["should": ranges.collect{it.toQuery()}]]
                    : ranges.first().toQuery()
        } catch (DateTimeParseException e) {
            throw new InvalidQueryException(e.getMessage())
        }
    }

    private class EndPoint {
        EndPoint(ParameterPrefix operator, String value) {
            this.operator = operator
            this.value = value
        }

        ParameterPrefix operator
        String value

        String elasticValue() {
            isDateField() ? QueryDateTime.parse(value).toElasticDateString() : value
        }

        boolean isWeek() {
            isDateField() && QueryDateTime.parse(value).precision == WEEK
        }

        @Override
        String toString() {
            return "${operator.prefix()}$fieldName=$value"
        }
    }

    private class Range {
        EndPoint min
        EndPoint max

        Range() {
            this(null, null)
        }

        Range(EndPoint min, EndPoint max) {
            this.min = min
            this.max = max
        }

        Map toQuery() {
            def conditions = [:]

            if (isDateField()) {
                if (min && max && min.isWeek() != max.isWeek()) {
                    throw new InvalidQueryException(
                            "Either both or none of range endpoints must be in week format: $min $max")
                }

                if (min?.isWeek() || max?.isWeek()) {
                    conditions['format'] = 'basic_week_date'
                }

                conditions['time_zone'] = timezone.getId()
            }

            if (min) {
                conditions[min.operator.asElasticRangeOperator()] = min.elasticValue()
            }

            if (max) {
                conditions[max.operator.asElasticRangeOperator()] = max.elasticValue()
            }

            ["range": [(fieldName): conditions]]
        }
    }
}