package whelk

class JsonValidator {

    private static JsonLd jsonLd

    JsonValidator(JsonLd jsonLd) {
        this.jsonLd = jsonLd
    }

    Set validate(Map obj) {
        Set<String> errors = new LinkedHashSet<>()
        doValidate(obj, errors)
        return errors
    }

    private void doValidate(Map obj, Set errors) {
        for (Object keyObj : obj.keySet()) {
            if (!(keyObj instanceof String)) {
                errors << "Invalid key: $keyObj"
                continue
            }

            String key = (String) keyObj
            Object value = obj[key]

            if ((key == jsonLd.ID_KEY || key == jsonLd.TYPE_KEY) && !(value instanceof String)) {
                errors << "Unexpected value of $key: ${value}"
                continue
            }

            Map termDefinition = jsonLd.vocabIndex[key] instanceof Map ? jsonLd.vocabIndex[key] : null
            if (!termDefinition && key.indexOf(':') > -1) {
                termDefinition = jsonLd.vocabIndex[jsonLd.expand(key)]
            }

            Map contextDefinition = jsonLd.context[key] instanceof Map ? jsonLd.context[key] : null
            boolean isVocabTerm = contextDefinition && contextDefinition[jsonLd.TYPE_KEY] == jsonLd.VOCAB_KEY

            if (!termDefinition && !jsonLd.LD_KEYS.contains(key)) {
                errors << "Unknown term: $key"
            }

            if ((key == jsonLd.TYPE_KEY || isVocabTerm)
                    && !vocabIndex.containsKey((String) value)) {
                errors << "Unknown vocab value for $key: $value"
            }

            boolean expectRepeat = key == jsonLd.GRAPH_KEY || key in jsonLd.getRepeatableTerms()
            boolean isRepeated = value instanceof List
            if (expectRepeat && !isRepeated) {
                errors << "Expected $key to be an array."
            } else if (!expectRepeat && isRepeated) {
                errors << "Unexpected array for $key."
            }

            List valueList = isRepeated ? (List) value : null
            if (valueList && valueList.size() == 0) {
                continue
            }
            Object firstValue = valueList?.getAt(0) ?: value
            boolean valueIsObject = firstValue instanceof Map

            if (firstValue && termDefinition
                    && termDefinition[jsonLd.TYPE_KEY] == 'ObjectProperty') {
                if (!isVocabTerm && !valueIsObject) {
                    errors << "Expected value type of $key to be object (value: $value)."
                } else if (isVocabTerm && valueIsObject) {
                    errors << "Expected value type of $key to be a vocab string (value: $value)."
                }
            }

            if (value instanceof List) {
                value.each {
                    if (it instanceof Map) {
                        doValidate((Map) it, errors)
                    }
                }
            } else if (value instanceof Map) {
                doValidate((Map) value, errors)
            }
        }
    }
}
