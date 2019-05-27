package whelk.rest.api

import whelk.JsonLd

/**
 * A utility to compute a list of boosted fields for ElasticSearch.
 * The fields are computed from lens definitions of chips ans cards. It is
 * currently contingent on the JsonLd.SEARCH_KEY mechanism to avoid the
 * repetition of nested fields.
 */
class ESQueryLensBoost {

    JsonLd jsonld

    ESQueryLensBoost(JsonLd jsonld) {
        this.jsonld = jsonld
    }

    List<String> computeBoostFieldsFromLenses(String[] types) {
        def boostFields = []
        def seenKeys = [] as Set

        def chipsLenses = jsonld.displayData.lensGroups?.chips
        def cardsLenses = jsonld.displayData.lensGroups?.cards

        boostFields += [ "$JsonLd.SEARCH_KEY^100" as String ]

        def baseTypes = ['Identity', 'Instance', 'Item']

        // Compute boost for "chip" properties.
        def selectedChipsLenses = selectLenses(
                chipsLenses, types, baseTypes, seenKeys)

        boostFields += selectedChipsLenses.sum { lens ->
            int boost = 200
            collectBoostFields(lens, boost, seenKeys)
        }

        // Compute boost for "card" properties.
        def selectedCardsLenses = selectLenses(
                cardsLenses, types, baseTypes, seenKeys)

        boostFields += selectedCardsLenses.sum { lens ->
            int boost = 10
            lens.showProperties.findResults {
                if (!(it instanceof String)) {
                    return
                }
                String key = it
                def termType = jsonld.vocabIndex.get(it)?.get(JsonLd.TYPE_KEY)

                // Follow the object property range to append chip properties
                // to the boosted path.
                if (termType == 'ObjectProperty') {
                    def dfn = jsonld.vocabIndex[key]

                    def rangeType = dfn.range ? dfn.range[0] : null
                    def rangeKey = rangeType ? jsonld.toTermKey(rangeType[JsonLd.ID_KEY]) : null
                    if (rangeKey &&
                        jsonld.isSubClassOf(rangeKey, 'QualifiedRole')) {
                        def chipLens = jsonld.getLensFor([(JsonLd.TYPE_KEY): rangeKey], chipsLenses)
                        return collectBoostFields(chipLens, boost, seenKeys).collect {
                            "${key}.$it" as String
                        }
                    } else {
                        key = "${key}.${JsonLd.SEARCH_KEY}"
                    }
                } else if (jsonld.isLangContainer(jsonld.context[it])) {
                    key = "${key}.${jsonld.locales[0]}"
                }

                if (key in seenKeys) {
                    return
                }
                seenKeys << key

                return "${key}^$boost" as String
            }.flatten()
        }

        return boostFields.unique()
    }

    List<String> collectBoostFields(lens, boost, seenKeys) {
        lens.showProperties.findResults {
            if (!(it instanceof String)) {
                return
            }
            String key = it
            def termType = jsonld.vocabIndex.get(it)?.get(JsonLd.TYPE_KEY)
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

    private List<Map> selectLenses(lenses, types, baseTypes, seenKeys) {
        if (types) {
            return types.collect {
                jsonld.getLensFor([(JsonLd.TYPE_KEY): it], lenses)
            }
        } else {
            return lenses?.lenses.values().findAll { lens ->
                baseTypes.any { jsonld.isSubClassOf(lens.classLensDomain, it) }
            }
        }
    }

}
