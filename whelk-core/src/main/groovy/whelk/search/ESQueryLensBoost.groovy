package whelk.search

import whelk.JsonLd

/**
 * A utility to compute a list of boosted fields for ElasticSearch.
 * The fields are computed from lens definitions of chips ans cards. It is
 * currently contingent on the JsonLd.SEARCH_KEY mechanism to avoid the
 * repetition of nested fields.
 */
class ESQueryLensBoost {

    static int CHIP_BOOST = 200
    static int STR_BOOST = 100
    static int CARD_BOOST = 10

    JsonLd jsonld

    ESQueryLensBoost(JsonLd jsonld) {
        this.jsonld = jsonld
    }

    def getChipsLenses() { jsonld.displayData.lensGroups?.chips }

    def getCardsLenses() { jsonld.displayData.lensGroups?.cards }

    List<String> computeBoostFieldsFromLenses(String[] types) {
        def boostFields = []
        def seenKeys = [] as Set

        boostFields += [ "$JsonLd.SEARCH_KEY^$STR_BOOST" as String ]

        def baseTypes = ['Identity', 'Instance', 'Item']

        // Compute boost for "chip" properties.
        def selectedChipsLenses = selectLenses(
                chipsLenses, types, baseTypes, seenKeys)

        boostFields += selectedChipsLenses.sum { lens ->
            collectBoostFields(lens, CHIP_BOOST, seenKeys)
        }

        // Compute boost for "card" properties.
        def selectedCardsLenses = selectLenses(
                cardsLenses, types, baseTypes, seenKeys)

        boostFields += selectedCardsLenses.sum { lens ->
            return getPropertiesToShow(lens).collect {
                computeCardPropertyBoosts(it, seenKeys)
            }.flatten()
        }

        return boostFields.unique().findAll()
    }

    private List<Map> selectLenses(lenses, types, baseTypes, seenKeys) {
        if (types) {
            return types.findResults {
                jsonld.getLensFor([(JsonLd.TYPE_KEY): it], lenses)
            }
        } else {
            return lenses?.lenses?.values()?.findAll { lens ->
                baseTypes.any { jsonld.isSubClassOf(lens.classLensDomain, it) }
            }
        }
    }

    private List<String> getPropertiesToShow(Map lens) {
        def properties = new LinkedHashSet()
        for (dfn in lens.showProperties) {
            if (dfn instanceof String) {
                properties << dfn
            } else if (dfn instanceof Map) {
                for (alt in dfn.alternateProperties) {
                    if (alt instanceof String) {
                        properties << alt
                    } else if (alt instanceof Map && alt.subPropertyOf) {
                        properties << alt.subPropertyOf
                    }
                }
            }
        }
        return properties as List
    }

    private List<String> collectBoostFields(Map lens, int boost, Set seenKeys) {
        getPropertiesToShow(lens).findResults {
            if (!(it instanceof String)) {
                return
            }
            String key = it
            String termType = jsonld.vocabIndex.get(it)?.get(JsonLd.TYPE_KEY)
            if (termType == 'ObjectProperty') {
                key = "${key}.${JsonLd.SEARCH_KEY}"
            } else if (jsonld.isLangContainer(jsonld.context[it])) {
                key = "${key}.${jsonld.locales[0]}"
            } else {
                // this property is part of the JsonLd.SEARCH_KEY value
                // but keep it anyway
            }

            if (key in seenKeys) {
                return
            }
            seenKeys << key

            return "${key}^$boost" as String
        }
    }

    private List<String> computeCardPropertyBoosts(String prop, seenKeys) {
        String key = prop
        Map dfn = jsonld.vocabIndex[prop]

        // Follow the object property range to append chip properties to the
        // boosted path.
        if (dfn && dfn[JsonLd.TYPE_KEY] == 'ObjectProperty') {
            Map rangeType = dfn.range instanceof List ? dfn.range[0] : dfn.range
            String rangeKey =
                rangeType ? jsonld.toTermKey(rangeType[JsonLd.ID_KEY]) : null
            if (rangeKey && jsonld.isSubClassOf(rangeKey, 'QualifiedRole')) {
                def obj = [(JsonLd.TYPE_KEY): rangeKey]
                def rangeChipLens = jsonld.getLensFor(obj, chipsLenses)
                def rangeChipFields = collectBoostFields(
                        rangeChipLens, CARD_BOOST, [] as Set)

                return rangeChipFields.collect { "${key}.$it" as String }
                        .findAll {!seenKeys.contains(it) }
            } else {
                key = "${key}.${JsonLd.SEARCH_KEY}"
            }
        } else if (jsonld.isLangContainer(jsonld.context[prop])) {
            key = "${key}.${jsonld.locales[0]}"
        }

        if (key in seenKeys) {
            return []
        }
        seenKeys << key

        return [ "${key}^$CARD_BOOST" as String ]
    }

}
