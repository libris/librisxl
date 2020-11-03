package whelk

import com.google.common.base.Preconditions
import whelk.exception.InvalidJsonException

class JsonValidator {
    private static JsonLd jsonLd

    private static final Set skipFields = ['_marcFailedFixedFields', '_marcUncompleted']

    private JsonValidator(JsonLd jsonLd) {
        this.jsonLd = jsonLd
    }

    static JsonValidator from(JsonLd jsonLd) {
        Preconditions.checkNotNull(jsonLd)
        Preconditions.checkArgument(!jsonLd.context.isEmpty())
        Preconditions.checkArgument(!jsonLd.vocabIndex.isEmpty())
        return new JsonValidator(jsonLd)
    }

    class Validation {
        Set errors
        Mode mode
        boolean seenGraph = false

        enum Mode {
            FAIL_FAST,
            LOG_ERROR
        }

        Validation(Mode mode) {
            this.errors = new LinkedHashSet()
            this.mode = mode
        }
    }

    void validate(Document doc) throws InvalidJsonException {
        doValidate(doc.data, new Validation(Validation.Mode.FAIL_FAST))
    }

    Set validateAll(Map map) {
        def validation = new Validation(Validation.Mode.LOG_ERROR)
        doValidate(map, validation)
        return validation.errors
    }

    private void doValidate(Map data, validation) {
        data.each { key, value ->
            if (key in skipFields || !passedPreValidation(key, value, validation)) {
                return
            }
            checkIsNotNestedGraph(key, value, validation)

            checkHasDefinition(key, validation)

            verifyVocabTerm(key, value, validation)

            validateRepeatability(key, value, validation)

            validateObjectProperties(key, value, validation)

            validateNext(value, validation)
        }
    }

    private passedPreValidation(key, value, validation) {
        return !(isUnexpected(key, value, validation) || keyIsInvalid(key, validation) || isEmptyValueList(value))
    }

    private checkIsNotNestedGraph(key, value, validation) {
        if (key == jsonLd.GRAPH_KEY) {
            if (validation.seenGraph) {
                handleError("Nested graph element: $value", validation)
            }
            validation.seenGraph = true
        }
    }

    private boolean keyIsInvalid(key, validation) {
        if(!(key instanceof String)) {
            handleError("Invalid key: $key", validation)
            return true
        } else {
            return false
        }
    }

    private boolean isEmptyValueList(value) {
        List valueList = isRepeated(value) ? (List) value : null
        return valueList && valueList.isEmpty()
    }

    private boolean isUnexpected(key, value, validation) { //Rename me
        if ((key == jsonLd.ID_KEY || key == jsonLd.TYPE_KEY) && !(value instanceof String)) {
            handleError("Unexpected value of $key: ${value}", validation)
            return true
        } else {
            return false
        }
    }

    private void checkHasDefinition(String key, validation) {
        if (!getTermDefinition(key) && !jsonLd.LD_KEYS.contains(key)) {
            handleError("Unknown term: $key", validation)
        }
    }

    private boolean isVocabTerm(String key) {
        def contextDefinition = getContextDefinition(key)
        boolean isVocabTerm = contextDefinition && contextDefinition[jsonLd.TYPE_KEY] == jsonLd.VOCAB_KEY
        return isVocabTerm
    }

    private void verifyVocabTerm(String key, value, validation) {
        if ((key == jsonLd.TYPE_KEY || isVocabTerm(key))
                && !jsonLd.vocabIndex.containsKey((String) value)) {
            handleError("Unknown vocab value for $key: $value", validation)
        }
    }

    private boolean isRepeated(value) {
        return value instanceof List
    }

    private void validateRepeatability(String key, value, validation) {
        boolean expectRepeat = key == jsonLd.GRAPH_KEY || key in jsonLd.getRepeatableTerms()
        if (expectRepeat && !isRepeated(value)) {
            handleError("Expected term $key to be an array. $key is declared as repeatable in context.", validation)
        } else if (!expectRepeat && isRepeated(value)) {
            handleError("Unexpected array for $key. $key is not declared as repeatable in context.", validation)
        }
    }

    private void validateObjectProperties(String key, value, validation) {
        List valueList = isRepeated(value) ? (List) value : null
        Object firstValue = valueList?.getAt(0) ?: value
        boolean valueIsObject = firstValue instanceof Map
        def termDefinition = getTermDefinition(key)
        if (firstValue && termDefinition
                && termDefinition[jsonLd.TYPE_KEY] == 'ObjectProperty') {
            if (!isVocabTerm(key) && !valueIsObject) {
                handleError("Expected value type of $key to be object (value: $value).", validation)
            } else if (isVocabTerm(key) && valueIsObject) {
                handleError("Expected value type of $key to be a vocab string (value: $value).", validation)
            }
        }
    }

    private Map getContextDefinition(String key) {
        return jsonLd.context[key] instanceof Map ? jsonLd.context[key] : null
    }

    private Map getTermDefinition(String key) {
        Map termDefinition = jsonLd.vocabIndex[key] instanceof Map ? jsonLd.vocabIndex[key] : null
        if (!termDefinition && key.indexOf(':') > -1) {
            termDefinition = jsonLd.vocabIndex[jsonLd.expand(key)]
        }
        return termDefinition
    }

    private void validateNext(value, Validation validation) {
        if (value instanceof List) {
            value.each {
                if (it instanceof Map) {
                    doValidate(it, validation)
                }
            }
        } else if (value instanceof Map) {
            doValidate(value, validation)
        }
    }

    private void handleError(String error, Validation validation) {
        if (validation.mode == Validation.Mode.FAIL_FAST) {
            throw new InvalidJsonException(error)
        } else if (validation.mode == Validation.Mode.LOG_ERROR) {
            validation.errors << error
        }
    }
}
