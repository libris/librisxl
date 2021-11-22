package whelk.search


import whelk.Whelk
import whelk.exception.InvalidQueryException

import java.time.ZoneId
import java.time.format.DateTimeParseException

import static RangeParameterPrefix.MATCHES
import static RangeParameterPrefix.MAX
import static RangeParameterPrefix.MAX_EX
import static RangeParameterPrefix.MIN
import static RangeParameterPrefix.MIN_EX
import static whelk.search.QueryDateTime.Precision.WEEK

class Ranges {
    String fieldName
    ZoneId timezone
    Whelk whelk

    private List<Query> clauses = []

    private Ranges(String fieldName, ZoneId timezone = null, Whelk whelk) {
        this.fieldName = fieldName
        this.timezone = timezone
        this.whelk = whelk
    }

    static Ranges nonDate(String fieldName, Whelk whelk) {
        new Ranges(fieldName, whelk)
    }

    static Ranges date(String fieldName, ZoneId timezone, Whelk whelk) {
        new Ranges(fieldName, timezone, whelk)
    }

    void add(RangeParameterPrefix operator, value) {
        switch (operator) {
            case MIN:
            case MIN_EX:
                Range openOrNew = (clauses.find {it instanceof Range && !it.min } ?: (clauses << new Range()).last()) as Range
                openOrNew.min = new EndPoint(operator, value)
                break

            case MAX:
            case MAX_EX:
                Range openOrNew = (clauses.find {it instanceof Range &&  !it.max } ?: (clauses << new Range()).last()) as Range
                openOrNew.max = new EndPoint(operator, value)
                break

            case MATCHES:
                clauses << (isDateField() ? new Range(new EndPoint(MIN, value), new EndPoint(MAX, value)) : new OrNarrower(value))
                break
        }
    }

    boolean isDateField() {
        timezone != null
    }

    Map toQuery() {
        if (clauses.isEmpty()) {
            throw new IllegalStateException("no ranges")
        }

        try {
            clauses.size() > 1
                    ? ["bool": ["should": clauses.collect{it.toQuery()}]]
                    : clauses.first().toQuery()
        } catch (DateTimeParseException e) {
            throw new InvalidQueryException(e.getMessage())
        }
    }

    private class EndPoint {
        EndPoint(RangeParameterPrefix operator, String value) {
            this.operator = operator
            this.value = value
        }

        RangeParameterPrefix operator
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

    private interface Query {
        Map toQuery()
    }
    
    private class Range implements Query {
        EndPoint min
        EndPoint max

        Range() {
            this(null, null)
        }

        Range(EndPoint min, EndPoint max) {
            this.min = min
            this.max = max
        }

        @Override
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

    private class OrNarrower implements Query {
        String value

        OrNarrower(String value) {
            this.value = value
        }
        
        @Override
        Map toQuery() {
            def values = [value] + whelk.relations.followReverseBroader(value).collect()
            [
                    "terms" : [
                            (fieldName) : values
                    ]
            ]
        }
    }
}