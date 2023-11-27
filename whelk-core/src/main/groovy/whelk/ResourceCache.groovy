package whelk

import groovy.transform.CompileStatic
import groovy.transform.NullCheck
import whelk.filter.BlankNodeLinker
import whelk.filter.LanguageLinker

// TODO: Make this a general interface for loading by type (or inScheme).
@CompileStatic
class ResourceCache {
    LanguageResources languageResources
    RelatorResources relatorResources

    @CompileStatic
    //@NullCheck(includeGenerated = true)
    static class LanguageResources {
        LanguageLinker languageLinker
        Map languages
        Map transformedLanguageForms
    }

    @CompileStatic
    static class RelatorResources {
        BlankNodeLinker relatorLinker
        Iterable<Map> relators
    }
}
