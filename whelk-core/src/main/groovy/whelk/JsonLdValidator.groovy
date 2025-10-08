package whelk

import org.apache.jena.iri.IRI
import org.apache.jena.iri.IRIFactory
import com.google.common.base.Preconditions
import whelk.util.DocumentUtil

class JsonLdValidator {
    private static JsonLd jsonLd
    private Collection skipTerms = []
    private Map<String, Validation.Scope> legacyScopes = [
            'auth': Validation.Scope.AUTH,
            'bib': Validation.Scope.BIB,
            'definitions': Validation.Scope.DEFINITIONS,
            'hold': Validation.Scope.HOLD,
    ]
    private Set langAliases
    
    static IRIFactory iriFactory = IRIFactory.iriImplementation()
    
    private JsonLdValidator(JsonLd jsonLd) {
        this.jsonLd = jsonLd
        langAliases = jsonLd.langContainerAlias.values() as Set
    }

    static JsonLdValidator from(JsonLd jsonLd) {
        Preconditions.checkNotNull(jsonLd)
        Preconditions.checkArgument(!jsonLd.context.isEmpty())
        Preconditions.checkArgument(!jsonLd.vocabIndex.isEmpty())
        def v = new JsonLdValidator(jsonLd)
        v.setSkipTerms(['_marcUncompleted', '_marcFailedFixedFields', JsonLd.Platform.CATEGORY_BY_COLLECTION])
        return v
    }

    class Validation {
        List<Error> errors = new ArrayList<>()
        boolean seenGraph = false
        List at
        Scope scope

        enum Scope {
            ALL,     // Every possible validation
            DEFAULT, // Validations enabled for all collection types
            AUTH,
            BIB,
            DEFINITIONS,
            HOLD,
        }

        Validation(Scope scope) {
            this.scope = scope
        }
    }

    List<Error> validateAll(Map map) {
        def validation = new Validation(Validation.Scope.ALL)
        doValidate(map, validation)
        return validation.errors
    }

    List<Error> validate(Map map) {
        return validate(map, null)
    }

    List<Error> validate(Map map, String collection) {
        def validation
        if (collection && legacyScopes.containsKey(collection)) {
            validation = new Validation(legacyScopes[collection])
        } else {
            validation = new Validation(Validation.Scope.DEFAULT)
        }
        doValidate(map, validation)
        return validation.errors
    }

    private void doValidate(Map data, Validation validation) {
        DocumentUtil.traverse(data, { value, path ->
            if (!path) {
                return
            }
            def key = path.last() as String

            if (!passedPreValidation(key, value, path, validation)) {
                return
            }
            validation.at = path.collect()

            if (checkLangContainer(path, key, value, validation)) {
                return 
            }
            
            if (validation.scope == Validation.Scope.ALL) {
                verifyAll(key, value, validation)
                return
            }

            // Validations enabled for all collections
            checkIsNotNestedGraph(key, value, validation)
            validateId(key, value, validation)

            // Additional per-collection validations
            switch (validation.scope) {
                case Validation.Scope.AUTH:
                    checkHasDefinition(key, validation)
                    validateObjectProperties(key, value, validation)
                    verifyVocabTerm(key, value, validation)
                    break
                case Validation.Scope.BIB:
                    checkHasDefinition(key, validation)
                    validateObjectProperties(key, value, validation)
                    break
                case Validation.Scope.DEFINITIONS:
                    break
                case Validation.Scope.HOLD:
                    checkHasDefinition(key, validation)
                    validateObjectProperties(key, value, validation)
                    verifyVocabTerm(key, value, validation)
                    break
            }
        })
    }

    private passedPreValidation(String key, value, path, validation) {
        return  !skipTermIsInPath(path) &&
                !key.isNumber() &&          // Continue if traverse is at a list element (key == 0, 1...)
                !(isUnexpected(key, value, validation) || keyIsInvalid(key, validation) || isEmptyValueList(value))
    }

    private void verifyAll(String key, value, Validation validation) {
        checkIsNotNestedGraph(key, value, validation)

        checkHasDefinition(key, validation)

        verifyVocabTerm(key, value, validation)

        validateRepeatability(key, value, validation)

        validateObjectProperties(key, value, validation)

        validateId(key, value, validation)
    }

    private void checkIsNotNestedGraph(String key, value, validation) {
        if (key == jsonLd.GRAPH_KEY) {
            if (validation.seenGraph) {
                handleError(new Error(Error.Type.NESTED_GRAPH, key, value), validation)
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
            handleError(new Error(Error.Type.UNEXPECTED, key, value), validation)
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
    
    private boolean checkLangContainer( List path, String key, value, validation) {
        if (langAliases.intersect(path)) {
            if (key in langAliases) {
                if (value !instanceof Map) {
                    handleError(new Error(Error.Type.OBJECT_TYPE_EXPECTED, key, value), validation)
                }
            } else if (value !instanceof String && value !instanceof List) {
                handleError(new Error(Error.Type.UNEXPECTED, key, value), validation)
            } else if (value instanceof List && value.any{ it !instanceof String }) {
                handleError(new Error(Error.Type.UNEXPECTED, key, value), validation)
            }
            return true
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
            handleError(new Error(Error.Type.UNKNOWN_VOCAB_VALUE, key, value), validation)
        }
    }

    private void validateRepeatability(String key, value, validation) {
        boolean expectRepeat = key == jsonLd.GRAPH_KEY || key in jsonLd.repeatableTerms
        if (expectRepeat && !isRepeated(value)) {
            handleError(new Error(Error.Type.ARRAY_EXPECTED, key, value), validation)
        } else if (!expectRepeat && isRepeated(value)) {
            handleError(new Error(Error.Type.UNEXPECTED_ARRAY, key, value), validation)
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
                handleError(new Error(Error.Type.OBJECT_TYPE_EXPECTED, key, value) , validation)
            } else if (isVocabTerm(key) && valueIsObject) {
                handleError(new Error(Error.Type.VOCAB_STRING_EXPECTED, key, value), validation)
            }
        }
    }

    private void validateId(String key, value, Validation validation) {
        if (key == jsonLd.ID_KEY) {
            IRI iri = iriFactory.create(value)
            if (iri.hasViolation(false)) {
                handleError(new Error(Error.Type.INVALID_IRI, key, value), validation)
            }
        }
    }

    private Map getContextDefinition(String key) {
        return jsonLd.context[key] instanceof Map ? jsonLd.context[key] : null
    }

    private boolean isRepeated(value) {
        return value instanceof List
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
        validation.errors << error
    }

    private Collection getUndefinedContextTerms() {
        return jsonLd.context.findAll {k, v -> v == null}.keySet()
    }

    void setSkipTerms(Collection terms) {
        this.skipTerms = terms
    }

    void skipUndefined() {
        setSkipTerms(getUndefinedContextTerms())
    }

    private boolean skipTermIsInPath(path) {
        path.any { skipTerms.contains(it) }
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
            INVALID_KEY("Invalid key"),
            INVALID_IRI("Invalid IRI")

            final String description

            private Type(String desc) {
                this.description = desc
            }
        }

        Type type
        List path

        private final String key
        private final Object value

        Error(Type type, String key, Object value = "") {
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

        Map toMap() {
            [
                    'type'       : type,
                    'description': type.description,
                    'path'       : path,
                    'key'        : key,
                    'value'      : value
            ]
        }
    }
}
