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
      marc: 'https://id.kb.se/marc',
    ]

    ResourceCache resourceCache

    Map<String, String> typeToCategory

    Map<String, Map<String, Object>> categories
    Map preferredCategory
    Map categoryMatches

    TypeCategoryNormalizer(ResourceCache resourceCache) {
        this.resourceCache = resourceCache
        typeToCategory = makeTypeToCategoryMapping(resourceCache.jsonld)
        computeMappings()
    }

    Map<String, String> makeTypeToCategoryMapping(JsonLd jsonld) {
        var mapping = new TreeMap()

        var categoryPropertyId = jsonld.vocabId + 'category'

        for (Map term : jsonld.vocabIndex.values()) {
            Map restriction = null
            String type = null
            if (term.intersectionOf) {
                type = term[ID]
                if (type.startsWith(jsonld.vocabId)) {
                    type = type.substring(jsonld.vocabId.length())
                }

                def basetype = term.intersectionOf[0][ID]
                if (basetype.startsWith(jsonld.vocabId)) {
                    basetype = basetype.substring(jsonld.vocabId.length())
                }
                if (basetype !in ['Monograph', 'Instance', 'PhysicalResource', 'DigitalResource']) {
                  continue
                }

                restriction = term.intersectionOf[1]
            }
            // restriction |= term.subClassOf.findResult { it.onProperty }
            // TODO: also (only?) just subClassOf
            if (type && restriction?.onProperty && restriction?.onProperty[ID] == categoryPropertyId) {
                var category = term.intersectionOf[1].hasValue
                mapping[type] = category[ID]
            }
        }

        return mapping
    }

    void computeMappings() {
        preferredCategory = new TreeMap()
        categoryMatches = new TreeMap()
        categories = new HashMap()

        categories.putAll(resourceCache.getByType('Category'))
        categories.putAll(resourceCache.getByType('marc:EnumeratedTerm'))

        var sourceSchemes = [SCHEMES.marc, SCHEMES.tgm, SCHEMES.saogf, SCHEMES.barngf]
        var targetSchemes = [SCHEMES.kbrda, SCHEMES.saogf, SCHEMES.ktg]
        var broaderSchemes = [SCHEMES.saogf, SCHEMES.barngf, SCHEMES.kbrda, SCHEMES.tgm, SCHEMES.ktg]

        for (ctg in categories.values()) {
            String id = ctg[ID]
            String scheme = ctg.inScheme?[ID]
            if (!scheme) {
                scheme = id.split('/[^/]+$')[0]
            }

            def closeMatch = asList(ctg.closeMatch).findResults { categories[it[ID]] }
            def exactMatch = asList(ctg.exactMatch).findResults { categories[it[ID]] }
            def broadMatch = asList(ctg.broadMatch).findResults { categories[it[ID]] }
            def broader = asList(ctg.broader).findResults { categories[it[ID]] }

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

    public static void main(String[] args) {
        var whelk = Whelk.createLoadedSearchWhelk()

        var catTypeNormalizer = new TypeCategoryNormalizer(whelk.resourceCache)

        var outfile = new File(args[0])
        var mappings = [
            typeToCategory: catTypeNormalizer.typeToCategory,
            preferredCategory: catTypeNormalizer.preferredCategory,
            categoryMatches: catTypeNormalizer.categoryMatches,
        ]
        whelk.util.Jackson.mapper.writerWithDefaultPrettyPrinter().writeValue(outfile, mappings)
    }

}
