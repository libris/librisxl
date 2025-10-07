package whelk.search

import whelk.JsonLd
import whelk.Whelk
import whelk.exception.InvalidQueryException
import groovy.util.logging.Log4j2 as Log

import java.time.ZoneId
import java.time.format.DateTimeParseException

import static RangeParameterPrefix.MATCHES
import static RangeParameterPrefix.MAX
import static RangeParameterPrefix.MAX_EX
import static RangeParameterPrefix.MIN
import static RangeParameterPrefix.MIN_EX
import static whelk.search.QueryDateTime.Precision.WEEK

@Log
class Ranges {
    String fieldName
    String fourDigitsFieldName
    ZoneId timezone
    Whelk whelk

    private List<Query> ranges = []

    private Ranges(String fieldName, String fourDigitsFieldName = null, ZoneId timezone = null, Whelk whelk) {
        this.fieldName = fieldName
        this.fourDigitsFieldName = fourDigitsFieldName
        this.timezone = timezone
        this.whelk = whelk
    }

    static Ranges nonDate(String fieldName, Whelk whelk) {
        new Ranges(fieldName, whelk)
    }

    static Ranges date(String fieldName, ZoneId timezone, Whelk whelk) {
        new Ranges(fieldName, null, timezone, whelk)
    }

    static Ranges fourDigits(String fieldName, String fourDigitsFieldName, Whelk whelk) {
        new Ranges(fieldName, fourDigitsFieldName, whelk)
    }

    void add(RangeParameterPrefix operator, value) {
        switch (operator) {
            case MIN:
            case MIN_EX:
                Range openOrNew = (ranges.find {it instanceof Range && !it.min } ?: (ranges << new Range()).last()) as Range
                openOrNew.min = new EndPoint(operator, value)
                break

            case MAX:
            case MAX_EX:
                Range openOrNew = (ranges.find {it instanceof Range &&  !it.max } ?: (ranges << new Range()).last()) as Range
                openOrNew.max = new EndPoint(operator, value)
                break

            case MATCHES:
                ranges << (isDateField() ? new Range(new EndPoint(MIN, value), new EndPoint(MAX, value)) : new OrNarrower(value))
                break
        }
    }

    boolean isDateField() {
        timezone != null
    }

    boolean isFourDigitField() {
        fourDigitsFieldName != null
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
        EndPoint(RangeParameterPrefix operator, String value) {
            this.operator = operator
            this.value = value
        }

        RangeParameterPrefix operator
        String value

        def elasticValue() {
            if (isDateField()) {
                return QueryDateTime.parse(value).toElasticDateString()
            }
            if (isFourDigitField() && isFourDigits(value)) {
                return Integer.parseInt(value)
            }
            return value
        }

        boolean isWeek() {
            isDateField() && QueryDateTime.parse(value).precision == WEEK
        }

        @Override
        String toString() {
            return "${operator.prefix()}$fieldName=$value"
        }

        private static isFourDigits(String s) {
            return s.matches("\\d{4}")
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

            ["range": [(fourDigitsFieldName ?: fieldName): conditions]]
        }
    }

    private class OrNarrower implements Query {
        String value

        OrNarrower(String value) {
            this.value = value
        }
        
        @Override
        Map toQuery() {
            def values = [value] + narrower(value)
            
            if (values.size() > whelk.elastic.maxTermsCount) {
                log.warn("narrower($value) gave more than ES maxTermsCount (${whelk.elastic.maxTermsCount}) results, truncating term list")
                values = values.take(whelk.elastic.maxTermsCount)
            }
            
            ["terms" : [(fieldName) : values]]
        }
        
        private Collection<String> narrower(String value) {
            def ld = whelk.jsonld
            def termKey = ld.toTermKey(value)
            if (termKey in ld.vocabIndex) {
                return ld.getSubClasses(termKey).findResults{ ld.toTermId(it) }
            } else {
                return whelk.relations.followReverseBroader(value).collect()
            }
        }
    }
}