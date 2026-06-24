package whelk.rest.api

import groovy.transform.CompileStatic
import groovy.xml.StreamingMarkupBuilder
import whelk.Document
import whelk.JsonLd
import whelk.search2.AppParams
import whelk.search2.Operator
import whelk.util.FresnelUtil

import java.time.Instant

import static groovy.transform.TypeCheckingMode.SKIP
import static whelk.JsonLd.ID_KEY
import static whelk.JsonLd.REVERSE_KEY
import static whelk.JsonLd.TYPE_KEY
import static whelk.JsonLd.asList
import static whelk.JsonLd.looksLikeIri

@CompileStatic
class SearchFeed {

    JsonLd jsonld
    FresnelUtil fresnelUtil
    List<String> locales

    Set<String> skipKeys = [ID_KEY, REVERSE_KEY, 'meta', 'reverseLinks', '_categoryByCollection'] as Set<String>

    SearchFeed(JsonLd jsonld, FresnelUtil fresnelUtil, List<String> locales) {
        this.jsonld = jsonld
        this.fresnelUtil = fresnelUtil
        this.locales = locales
    }

    @CompileStatic(SKIP)
    String represent(String feedId, Object searchResults) {
        var timestamps = searchResults.items
                .findResults { it?.meta?.modified?.with { Document.parseTimestamp(String.valueOf(it))} }
                .sort()
        var lastMod = Document.formatTimeStamp(!timestamps.isEmpty() ? timestamps.last() : Instant.now())
        var feedTitle = buildTitle(searchResults)
        return new StreamingMarkupBuilder().bind { mb ->
            feed(xmlns: 'http://www.w3.org/2005/Atom') {
                title(feedTitle)
                id(feedId)
                author {
                  name("Libris")
                }
                link(rel: 'self', href: searchResults[ID_KEY])
                for (rel in ['next', 'prev', 'first', 'last']) {
                    def ref = searchResults[rel]
                    if (ref) {
                        link(rel: rel, href: ref[ID_KEY])
                    }
                }
                updated(lastMod)
                for (item in searchResults.items) {
                    entry {
                        id(item[ID_KEY])
                        link(rel: 'alternate', type: 'text/html', href: item[ID_KEY])
                        updated(item.meta.modified)
                        title(toChipString(item))
                        summary(type: 'xhtml') {
                            toEntryCard(mb, item)
                        }
                        content(src: item[ID_KEY])
                    }
                }
            }
        }.toString()
    }

    @CompileStatic(SKIP)
    String buildTitle(Map searchResults) {
        var title = getByLang((Map) searchResults['titleByLang'])
        def params = searchResults.search?.mapping?.findResults {
            searchMappingToString(it as Map)
        }
        if (params) {
            return title + ' | ' + params.join(' ')
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
            if(!item[TYPE_KEY]) {
                return
            }
            var sorted = fresnelUtil.mapThroughLens(item, FresnelUtil.NestedLenses.CARD_TO_CHIP_TO_TOKEN, [], [])
            for (kv in sorted) {
                div {
                    if (kv.key !in skipKeys) {
                        var label = getLabelFor(kv.key)
                        var values = asList(kv.value).findResults { toValueString(it) ?: null }
                        if (label && values) {
                            span(style: 'display: block; font-size: 0.75rem; margin-top: 0.5rem;') {
                                span(label)
                                span(style: 'display: none', ": ") // fallback when no CSS support
                            }
                            span {
                                values.eachWithIndex { v, i ->
                                    span(style: 'display: block') {
                                        if (i > 0) {
                                            span(style: 'display: none', ", ") // fallback when no CSS support
                                        }
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
        fresnelUtil.asFormattedString(item, FresnelUtil.NestedLenses.CHIP_TO_TOKEN, locales.first())
    }

    String toValueString(Object item) {
        if (item instanceof Map) {
            if (item[TYPE_KEY]) {
                return toChipString(item)
            }
            if (item[ID_KEY]) {
                return uriSlug(String.valueOf(item[ID_KEY]))
            }
        }

        return String.valueOf(item)
    }

    static String uriSlug(String s) {
        s.split('/').last()
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
        if (byLang == null) {
            return null
        }

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

    String searchMappingToString(Map m) {
        if (m.variable == AppParams.DEFAULT_SITE_FILTERS) {
            return null
        }

        if (m.object) {
            if (m.predicate && m.predicate['label']) {
                String l = m.predicate['label']
                l = looksLikeIri(l) ? uriSlug(l) : l
                return '& ' + l + '=' + toValueString(m.object)
            }
            return toValueString(m.object)
        }

        if ((m.value && m.value !instanceof Boolean)) {
            return toValueString(m.value)
        }

        for (var o : Operator.values()) {
            if (m[o.termKey]) {
                if (m.property[ID_KEY] == 'https://id.kb.se/vocab/textQuery'
                        || asList(m.property['category']).contains([(ID_KEY): 'https://id.kb.se/vocab/impliedByObject'])) {
                    return toValueString(m[o.termKey])
                }

                return o.format(toValueString(m.property), toValueString(m[o.termKey]))
            }
        }

        if (m.and) {
            return m.and.collect { searchMappingToString(it as Map) }.join(' AND ')
        }
        if (m.or) {
            return m.and.collect { searchMappingToString(it as Map) }.join(' OR ')
        }
        if (m.not) {
            return 'NOT ' + searchMappingToString(m.not as Map)
        }

        return null
    }
}
