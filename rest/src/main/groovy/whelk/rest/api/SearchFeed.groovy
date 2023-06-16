package whelk.rest.api

import groovy.transform.CompileStatic
import static groovy.transform.TypeCheckingMode.SKIP

import groovy.xml.MarkupBuilder
import groovy.xml.StreamingMarkupBuilder

import whelk.JsonLd
import static whelk.JsonLd.ID_KEY
import static whelk.JsonLd.TYPE_KEY
import static whelk.JsonLd.REVERSE_KEY
import static whelk.JsonLd.asList

@CompileStatic
class SearchFeed {

    JsonLd jsonld
    List<String> locales

    Set<String> skipKeys = [ID_KEY, REVERSE_KEY, 'meta', 'reverseLinks'] as Set
    Set<String> skipDetails = skipKeys + ([TYPE_KEY, 'commentByLang'] as Set)

    String feedTitle

    SearchFeed(JsonLd jsonld, List<String> locales) {
        this.jsonld = jsonld
        this.locales = locales
    }

    @CompileStatic(SKIP)
    String represent(String feedId, Object searchResults) {
        var lastMod = searchResults.items?[0]?.meta?.modified
        var feedTitle = buildTitle(searchResults)
        return new StreamingMarkupBuilder().bind { mb ->
            feed(xmlns: 'http://www.w3.org/2005/Atom') {
                title(feedTitle)
                id(feedId)
                link(rel: 'self', href: searchResults[ID_KEY])
                for (rel in ['next', 'prev', 'first', 'last']) {
                    def ref = searchResults[rel]
                    if (ref) {
                        link(rel: rel, href: ref[ID_KEY])
                    }
                }
                if (lastMod) updated(lastMod)
                for (item in searchResults.items) {
                    entry {
                        id(item[ID_KEY])
                        link(rel: 'alternate', type: 'text/html', href: item[ID_KEY])
                        updated(item.meta.modified)
                        title(toChipString(item))
                        summary(type: 'xhtml') {
                            toEntryCard(mb, item)
                        }
                        content(href: item[ID_KEY])
                    }
                }
            }
        }.toString()
    }

    @CompileStatic(SKIP)
    String buildTitle(Map searchResults) {
        var title = getByLang((Map) searchResults['titleByLang'])
        def params = searchResults.search?.mapping?.findResults {
            var o = toValueString(it.object, skipDetails)
            return o
        }
        if (params) {
            return title + ': ' + params.join(' & ')
        } else {
            return title
        }
    }

    @CompileStatic(SKIP)
    void toEntryCard(mb, Map item) {
        mb.div(xmlns: 'http://www.w3.org/1999/xhtml') {
            asList(item.meta?.hasChangeNote).each { note ->
                p { b(toChipString(note)) }
            }
            for (kv in item) {
                div {
                    if (kv.key !in skipKeys) {
                        var label = getLabelFor(kv.key)
                        var values = getValues(kv.value, kv.key)
                        if (label && values) {
                            span(label + ": ")
                            span {
                                values.eachWithIndex { v, i ->
                                    if (i > 0) {
                                        span(", " + v)
                                    } else {
                                        span(v)
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    String toChipString(Object item) {
        if (item instanceof Map) {
            def chip = jsonld.toChip(item)
            return toValueString(chip)
        } else {
            return item.toString()
        }
    }

    String toValueString( Object o, Set skipKeys=skipKeys) {
        var sb = new StringBuilder()
        buildValueString(sb, o, skipKeys)
        return sb.toString()
    }

    void buildValueString(StringBuilder sb, Object o, Set skipKeys=skipKeys) {
        if (o instanceof List) {
          for (v in o) buildValueString(sb, v, skipKeys)
        } else if (o instanceof Map) {
          for (kv in o) {
            if (kv.key !in skipKeys) {
              buildValueString(sb, getValues(kv.value, (String) kv.key), skipKeys)
            }
          }
        } else {
          if (sb.size() > 0) sb.append(" • ")
          sb.append(o.toString())
        }
    }

    List<String> getValues(Object o, String viaKey) {
        if (viaKey == TYPE_KEY || jsonld.isVocabTerm(viaKey)) {
            return asList(o).collect { getLabelFor((String) it) }
        } else if (jsonld.isLangContainer(jsonld.context[viaKey])) {
            return (List<String>) asList(o).findResults { getByLang((Map) it) }
        } else {
            return (List<String>) asList(o).findResults { toChipString(it) ?: null }
        }
    }

    String getLabelFor(String key) {
        String lookup = key == TYPE_KEY ? 'rdf:type' : key
        def term = jsonld.vocabIndex[lookup]
        if (term instanceof Map) {
            def byLang = term.get('labelByLang')
            if (byLang instanceof Map) {
                String s = getByLang(byLang)
                if (s) {
                    return s[0].toUpperCase() + s.substring(1)
                }
            }
        }
        return key
    }

    String getByLang(Map byLang) {
        for (lang in locales) {
            if (lang in byLang) {
                def o = byLang[lang]
                if (o instanceof String) {
                    return o
                } else if (o instanceof List && o.size() > 0) {
                    return o.get(0).toString()
                }
            }
        }
        for (value in byLang.values()) {
            return value
        }
        return null
    }
}
