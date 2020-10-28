package whelk

import com.google.common.base.Preconditions

class JsonValidator {
    private static JsonLd jsonLd
    private Set errors

    JsonValidator(JsonLd jsonLd) {
        this.jsonLd = Preconditions.checkNotNull(jsonLd)
        this.errors = new LinkedHashSet<>()
    }

    Set validate(Document doc) {
        doValidate(doc.data)
        return errors
    }

    private void doValidate(Map obj) {
        obj.each { key, value ->
            if (!passedPreValidation(key, value)) {
                return
            }
            checkHasDefinition(key)

            checkVocabTermInVocab(key, value)

            validateRepeatability(key, value)

            validateObjectProperties(key, value)

            validateNext(value)
        }
    }

    private passedPreValidation(key, value) {
        return !(isUnexpected(key, value) || keyIsInvalid(key) || isEmptyValueList(value))
    }

    private boolean keyIsInvalid(key) {
        if(!(key instanceof String)) {
            errors << "Invalid key: $key"
            return true
        } else {
            return false
        }
    }

    private boolean isEmptyValueList(value) {
        List valueList = isRepeated(value) ? (List) value : null
        return valueList && valueList.isEmpty()
    }

    private boolean isUnexpected(key, value) { //Rename me
        if ((key == jsonLd.ID_KEY || key == jsonLd.TYPE_KEY) && !(value instanceof String)) {
            errors << "Unexpected value of $key: ${value}"
            return true
        } else {
            return false
        }
    }

    private boolean isVocabTerm(String key) {
        def contextDefinition = getContextDefinition(key)
        boolean isVocabTerm = contextDefinition && contextDefinition[jsonLd.TYPE_KEY] == jsonLd.VOCAB_KEY
        return isVocabTerm
    }

    private boolean isRepeated(value) {
        return value instanceof List
    }

    private void validateObjectProperties(String key, value) {
        List valueList = isRepeated(value) ? (List) value : null
        Object firstValue = valueList?.getAt(0) ?: value
        boolean valueIsObject = firstValue instanceof Map
        def termDefinition = getTermDefinition(key)
        if (firstValue && termDefinition
                && termDefinition[jsonLd.TYPE_KEY] == 'ObjectProperty') {
            if (!isVocabTerm(key) && !valueIsObject) {
                errors << "Expected value type of $key to be object (value: $value)."
            } else if (isVocabTerm(key) && valueIsObject) {
                errors << "Expected value type of $key to be a vocab string (value: $value)."
            }
        }
    }

    private void validateNext(value) {
        if (value instanceof List) {
            value.each {
                if (it instanceof Map) {
                    doValidate(it)
                }
            }
        } else if (value instanceof Map) {
            doValidate(value)
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

    private void checkVocabTermInVocab(String key, value) {
        if ((key == jsonLd.TYPE_KEY || isVocabTerm(key))
                && !jsonLd.vocabIndex.containsKey((String) value)) {
            errors << "Unknown vocab value for $key: $value"
        }
    }

    private void checkHasDefinition(String key) {
        if (!getTermDefinition(key) && !jsonLd.LD_KEYS.contains(key)) {
            errors << "Unknown term: $key"
        }
    }

    private void validateRepeatability(String key, value) {
        boolean expectRepeat = key == jsonLd.GRAPH_KEY || key in jsonLd.getRepeatableTerms()
        if (expectRepeat && !isRepeated(value)) {
            errors << "Expected term $key to be an array. $key is declared as repeatable in context."
        } else if (!expectRepeat && isRepeated(value)) {
            errors << "Unexpected array for $key. $key is not declared as repeatable in context."
        }
    }
}
