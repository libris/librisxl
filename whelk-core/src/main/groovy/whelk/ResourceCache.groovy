package whelk

import groovy.transform.CompileStatic
import groovy.transform.NullCheck

import whelk.filter.LanguageLinker

// TODO: Make this a general interface for loading by type (or inScheme).
@CompileStatic
class ResourceCache {
    LanguageResources languageResources
    Iterable<Map> relators

    @CompileStatic
    //@NullCheck(includeGenerated = true)
    static class LanguageResources {
        LanguageLinker languageLinker
        Map languages
        Map transformedLanguageForms
    }
}
