package whelk

import com.google.common.base.Preconditions
import whelk.exception.InvalidJsonException
import whelk.util.DocumentUtil

class JsonValidator {
    private static JsonLd jsonLd

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
        List<Error> errors
        Mode mode
        boolean seenGraph = false
        List at

        enum Mode {
            FAIL_FAST,
            LOG_ERROR
        }

        Validation(Mode mode) {
            this.errors = new ArrayList<>()
            this.mode = mode
        }
    }

    void validate(Document doc) throws InvalidJsonException {
        doValidate(doc.data, new Validation(Validation.Mode.FAIL_FAST))
    }

    Set validateAll(Map map) {
        def validation = new Validation(Validation.Mode.LOG_ERROR)
        doValidate(map, validation)
        return validation.errors.collect {it.toStringWithPath()}.toSet()
    }

    List<Error> validate(Map map) {
        def validation = new Validation(Validation.Mode.LOG_ERROR)
        doValidate(map, validation)
        return validation.errors
    }

    private void doValidate(Map data, Validation validation) {
        DocumentUtil.traverse(data, { value, path ->
            if (!path) {
                return
            }
            def key = path.last() as String

            if (!passedPreValidation(key, value, validation)) {
                return
            }
            validation.at = path

            checkIsNotNestedGraph(key, value, validation)

            checkHasDefinition(key, validation)

            verifyVocabTerm(key, value, validation)

            validateRepeatability(key, value, validation)

            validateObjectProperties(key, value, validation)
        })
    }

    private passedPreValidation(String key, value, validation) {
        boolean keyInMap = !(key.isNumber() && value instanceof Map)
        return keyInMap || !(isUnexpected(key, value, validation) || keyIsInvalid(key, validation) || isEmptyValueList(value))
    }

    private checkIsNotNestedGraph(String key, value, validation) {
        if (key == jsonLd.GRAPH_KEY) {
            if (validation.seenGraph) {
                handleError(new Error(Error.Type.NESTED_GRAPH, key, value?.toString()), validation)
            }
            validation.seenGraph = true
        }
    }

    private boolean keyIsInvalid(String key, validation) {
        if(!(key instanceof String)) {
            handleError(new Error(Error.Type.INVALID_KEY, key), validation)
            return true
        } else {
            return false
        }
    }

    private boolean isEmptyValueList(value) {
        List valueList = isRepeated(value) ? (List) value : null
        return valueList && valueList.isEmpty()
    }

    private boolean isUnexpected(String key, value, validation) { //Rename me
        if ((key == jsonLd.ID_KEY || key == jsonLd.TYPE_KEY) && !(value instanceof String)) {
            handleError(new Error(Error.Type.UNEXPECTED, key), validation)
            return true
        } else {
            return false
        }
    }

    private void checkHasDefinition(String key, validation) {
        if (!getTermDefinition(key) && !jsonLd.LD_KEYS.contains(key)) {
            handleError(new Error(Error.Type.MISSING_DEFINITION, key), validation)
        }
    }

    private boolean isVocabTerm(String key) {
        def contextDefinition = getContextDefinition(key)
        boolean isVocabTerm = contextDefinition && contextDefinition[jsonLd.TYPE_KEY] == jsonLd.VOCAB_KEY
        return isVocabTerm
    }

    private void verifyVocabTerm(String key, value, validation) {
        if ((key == jsonLd.TYPE_KEY || isVocabTerm(key))
                && !jsonLd.vocabIndex.containsKey(value?.toString())) {
            handleError(new Error(Error.Type.UNKNOWN_VOCAB_VALUE, key, value?.toString()), validation)
        }
    }

    private boolean isRepeated(value) {
        return value instanceof List
    }

    private void validateRepeatability(String key, value, validation) {
        boolean expectRepeat = key == jsonLd.GRAPH_KEY || key in jsonLd.getRepeatableTerms()
        if (expectRepeat && !isRepeated(value)) {
            handleError(new Error(Error.Type.ARRAY_EXPECTED, key, value?.toString()), validation)
        } else if (!expectRepeat && isRepeated(value)) {
            handleError(new Error(Error.Type.UNEXPECTED_ARRAY, key, value?.toString()), validation)
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
                handleError(new Error(Error.Type.OBJECT_TYPE_EXPECTED, key, value?.toString()) , validation)
            } else if (isVocabTerm(key) && valueIsObject) {
                handleError(new Error(Error.Type.VOCAB_STRING_EXPECTED, key, value?.toString()), validation)
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

    private void handleError(Error error, Validation validation) {
        error.path = validation.at
        if (validation.mode == Validation.Mode.FAIL_FAST) {
            throw new InvalidJsonException(error.toString())
        } else if (validation.mode == Validation.Mode.LOG_ERROR) {
            validation.errors << error
        }
    }

    class Error {
        enum Type {
            VOCAB_STRING_EXPECTED("Expected value type to be a vocab string"),
            UNEXPECTED_ARRAY("Unexpected array. Key is not declared as repeatable in context"),
            ARRAY_EXPECTED("Expected term to be an array. Key is declared as repeatable in context"),
            OBJECT_TYPE_EXPECTED("Expected value type of key to be object"),
            UNKNOWN_VOCAB_VALUE("Unknown vocab value"),
            MISSING_DEFINITION("Unknown term. Missing definition"),
            UNEXPECTED("Unexpected value of key"),
            NESTED_GRAPH("Nested graph object found"),
            INVALID_KEY("Invalid key")

            final String description

            private Type(String desc) {
                this.description = desc
            }
        }

        private Type type
        private final String key
        private final String value
        List path

        Error(Type type, String key, String value = "") {
            this.type = type
            this.key = key
            this.value = value
        }

        String getDescription() {
            return type.description
        }

        String toStringWithPath() {
            return type.description +" at path: $path for KEY: $key VALUE: $value"
        }

        String toString() {
            return type.description +" for KEY: $key VALUE: $value"
        }
    }
}
