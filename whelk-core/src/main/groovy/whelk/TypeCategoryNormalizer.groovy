package whelk

import static whelk.JsonLd.ID_KEY as ID
import static whelk.JsonLd.TYPE_KEY as TYPE
import static whelk.JsonLd.asList

class TypeCategoryNormalizer {

    static Map SCHEMES = [
      saogf: 'https://id.kb.se/term/saogf',
      barngf: 'https://id.kb.se/term/barngf',
      tgm: 'https://id.kb.se/term/gmgpc/swe',
      kbrda: 'https://id.kb.se/term/rda',
      ktg: 'https://id.kb.se/term/ktg',
      saobf: 'https://id.kb.se/term/saobf',
      marc: 'https://id.kb.se/marc',
    ]

    ResourceCache resourceCache

    String categoryPropertyId
    Map<String, String> typeToCategory

    Map<String, Map<String, Object>> categories
    Map preferredCategory
    Map categoryMatches

    TypeCategoryNormalizer(ResourceCache resourceCache) {
        this.resourceCache = resourceCache
        categoryPropertyId = resourceCache.jsonld.vocabId + 'category'

        loadCategories()
        computeMappings()
    }

    JsonLd getJsonld() {
        return resourceCache.jsonld
    }

    Map<String, String> makeTypeToCategoryMapping() {
        var mapping = new TreeMap()

        for (Map term : jsonld.vocabIndex.values()) {
            Map catRestriction = null
            String type = null

            type = jsonld.toTermKey(term[ID])

            if (term.intersectionOf) {
                if (!isWorkOrInstanceClass(type)) {
                    var expectedbase = false
                    for (item in term.intersectionOf) {
                        if (ID in item && isWorkOrInstanceClass(jsonld.toTermKey(item[ID]))) {
                            expectedbase = true
                        }
                    }
                    if (!expectedbase) {
                        continue
                    }
                }
                catRestriction = term.intersectionOf.find { isCategoryRestriction(it) }
            } else if (term.equivalentClass || term.isSubClassOf) {
                if (!isWorkOrInstanceClass(type)) {
                  continue
                }
                // IMPORTANT: assuming that we only need to check direct equivalencies
                catRestriction = asList(term.equivalentClass).find { isCategoryRestriction(it) }
                if (!catRestriction) {
                    // IMPORTANT: assuming that we only need to check direct baseclasses
                    catRestriction = asList(term.isSubClassOf).find { isCategoryRestriction(it) }
                }
            }

            if (type && catRestriction) {
                var category = catRestriction.hasValue
                mapping[type] = category[ID]
            }
        }

        return mapping
    }

    boolean isWorkOrInstanceClass(String termId) {
        if (jsonld.isSubClassOf(termId, 'Work')) {
            return true
        }
        if (jsonld.isSubClassOf(termId, 'Instance')) {
            return true
        }
        return false
    }

    boolean isCategoryRestriction(Map term) {
        return term?.onProperty && term?.onProperty[ID] == categoryPropertyId
    }

    void loadCategories() {
        categories = new TreeMap()
        categories.putAll(resourceCache.getByType('Category'))
        categories.putAll(resourceCache.getByType('marc:EnumeratedTerm'))
    }

    void computeMappings() {
        typeToCategory = makeTypeToCategoryMapping()
        preferredCategory = new TreeMap()
        categoryMatches = new TreeMap()

        var sourceSchemes = [SCHEMES.marc, SCHEMES.tgm, SCHEMES.saogf, SCHEMES.barngf]
        var targetSchemes = [SCHEMES.kbrda, SCHEMES.saogf, SCHEMES.ktg, SCHEMES.saobf]
        var broaderSchemes = [SCHEMES.saogf, SCHEMES.barngf, SCHEMES.kbrda, SCHEMES.tgm, SCHEMES.ktg, SCHEMES.saobf]

        for (ctg in categories.values()) {
            String id = ctg[ID]
            String scheme = ctg.inScheme?[ID]
            if (!scheme) {
                scheme = id.split('/[^/]+$')[0]
            }

            def closeMatch = asList(ctg.closeMatch).findResults { if (ID in it) categories[it[ID]] }
            def exactMatch = asList(ctg.exactMatch).findResults { if (ID in it) categories[it[ID]] }
            def broadMatch = asList(ctg.broadMatch).findResults { if (ID in it) categories[it[ID]] }
            def broader = asList(ctg.broader).findResults { if (ID in it) categories[it[ID]] }

            for (match in closeMatch + exactMatch) {
                String matchId = match[ID]
                String matchScheme = match.inScheme?[ID]
                if (!matchScheme) {
                    matchScheme = matchId.split('/[^/]+$')[0]
                }

                if (scheme in sourceSchemes) {
                    if (matchScheme in targetSchemes) {
                        preferredCategory[id] = matchId
                    }
                }
                if (matchScheme in sourceSchemes) {
                    if (scheme in targetSchemes) {
                        preferredCategory[matchId] = id
                    }
                }
            }

            for (broadTerm in exactMatch + closeMatch + broadMatch + broader) {
                String broadTermScheme = broadTerm.inScheme?[ID]
                if (!broadTermScheme) {
                    broadTermScheme = broadTerm[ID].split('/[^/]+$')[0]
                }

                if (broadTermScheme in broaderSchemes) {
                    categoryMatches.get(id, []) << broadTerm[ID]
                    categoryMatches.get(id).sort().unique()
                }
            }
        }
    }

}
