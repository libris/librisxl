package datatool.util

class LanguageMapper {
    List ignoreCodes = []
    Map languageMap = [:]
    Map<String, List> ambiguousLabels = [:]
    Map substitutions = [:]
    Statistics stats

    LanguageMapper(List ignoreCodes = [], Statistics stats = null) {
        this.ignoreCodes = ignoreCodes
        this.stats = stats
    }

    void addLanguageDefinition(Map language) {
        String code = language['code'].toLowerCase()
        if (ignoreCodes.contains(code)) {
            return
        }

        Set<String> labels = [code]

        def prefLabels = language.prefLabelByLang as Map ?: [:]
        prefLabels.values().each(maybeCollection({ String label ->
            labels.add(label.toLowerCase())
        }))

        def altLabels = language.altLabelByLang as Map ?: [:]
        altLabels.values().each(maybeCollection({ String label ->
            labels.add(label.toLowerCase())
        }))

        String id = language['@id']
        labels.each { addMapping(it, id) }
    }

    void addMapping(String from, String to) {
        from = from.toLowerCase()
        if (ambiguousLabels.containsKey(from)) {
            ambiguousLabels[from] << to
        } else if (languageMap.containsKey(from)) {
            ambiguousLabels[from] = [to, languageMap.remove(from)]
        } else {
            languageMap[from] = to
        }
    }

    void addSubstitutions(Map s) {
        substitutions.putAll(s)
    }

    boolean knownId(String id) {
        return languageMap.values().contains(id)
    }

    List<Map> mapBlankLanguage(Map language, List existingLinks) {
        if (!language['label'] && !language['code']) {
            incrementCounter('unhandled shape', language.keySet())
            throw new RuntimeException('unhandled shape: ' + language.keySet())
        }

        String key = language['label'] ? 'label' : 'code'
        List<String> links = findLinks(language[key], existingLinks)
        if (links) {
            incrementCounter('mapped', language[key])
            return links.collect { ['@id': it] }
        }
        else {
            incrementCounter('not mapped', language[key])
        }

        if (language['sameAs'] && !language['sameAs'].any { knownId(it['@id']) }) {
            incrementCounter('sameAs 404 - removed', language['sameAs'])
            Map r = new HashMap(language)
            r.remove('sameAs')
            return [r]
        }

        return null
    }

    private List<String> findLinks(labelOrCode, List existingLinks) {
        if (labelOrCode instanceof String && findLink(labelOrCode, existingLinks)) {
            return [findLink(labelOrCode, existingLinks)]
        }

        List multiple = split(labelOrCode)
        if (multiple && multiple.every { findLinks(it, existingLinks) != null }) {
            return multiple.collect { findLinks(it, existingLinks) }.flatten()
        }

        return null
    }

    private String findLink(String s, List existingLinks) {
        s = canonize(s)
        if (languageMap.containsKey(s)) {
            return languageMap[s]
        }
        if (ambiguousLabels.containsKey(s)) {
            for (String id : ambiguousLabels[s]) {
                if (existingLinks.contains(id)) {
                    return id
                }
            }
        }
        return null
    }

    private List split(labelOrCode) {
        if (labelOrCode instanceof List) {
            return labelOrCode
        }

        // concatenated language codes, e.g "sweruseng", "swe ; rus ; eng"
        if (labelOrCode ==~ /^(\w{3}\W*){2,}/) {
            def m = labelOrCode =~ /(\w{3})\W*/
            def matches = []
            while (m.find()) {
                matches << m.group(1)
            }
            return matches
        }

        // e.g "Swedish, Russian and English"
        def m = labelOrCode =~ /(.+)(?: och | and |&)(.+)/
        if (m.matches()) {
            def matches = []
            for (String l : m.group(1).split(',')) {
                matches.add(trim(l))
            }
            matches.add(trim(m.group(2)))
            return matches
        }

        return []
    }

    private String canonize(String s) {
        s = trim(s.toLowerCase())
        if (substitutions.containsKey(s)) {
            s = substitutions[s]
        }
        return s
    }

    private String trim(String s) {
        // remove leading and trailing non-"alpha, digit or parentheses"
        def w = /\(\)\p{IsAlphabetic}\p{Digit}/
        def m = s =~ /[^${w}]*([${w} ]*[${w}])[^${w}]*/
        return m.matches() ? m.group(1) : s
    }

    Closure maybeCollection(Closure<?> c) {
        return { o ->
            if (o instanceof Collection) {
                o.each(c)
            } else {
                c.call(o)
            }
        }
    }

    private void incrementCounter(String category, Object name) {
        if (stats) {
            stats.increment(category, name)
        }
    }
}