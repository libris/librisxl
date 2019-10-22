package datatool.util


import static DocumentSearch.NOP
import static DocumentSearch.Operation
import static DocumentSearch.Replace

class LanguageLinker {
    private static final Closure isBlank = { !it.containsKey('@id') }
    LanguageMapper mapper

    LanguageLinker(LanguageMapper mapper) {
        this.mapper = mapper
    }

    boolean linkLanguages(data) {
        return new DocumentSearch().search(data) { path, value ->
            if (path && path.last().equals('language')) {
                return replaceBlankNode(value)
            }
            return NOP
        }
    }

    private Operation replaceBlankNode(List nodes) {
        if (!nodes.any(isBlank)) {
            return NOP
        }

        List existingLinks = nodes.findAll { !isBlank(it) }.collect { it['@id'] }
        List result = []

        List newLinked
        for (node in nodes) {
            if (isBlank(node) && (newLinked = mapper.mapBlankLanguage(node, existingLinks))) {
                result.addAll(newLinked.findAll { l ->
                    !existingLinks.contains(l['@id']) && !result.contains { it['@id'] == l['@id'] }
                })
            } else {
                result.add(node)
            }
        }

        if (nodes != result) {
            return new Replace(result)
        } else {
            return NOP
        }
    }

    private Operation replaceBlankNode(Map node) {
        List replacement
        if (isBlank(node) && (replacement = mapper.mapBlankLanguage(node, []))) {
            return replacement.size() > 1 ? new Replace(replacement) : new Replace(replacement[0])
        }
        return NOP
    }
}
