import whelk.converter.marc.RestructPropertyValuesAndFlagStep

merger = new RestructPropertyValuesAndFlagStep(
    matchValuePattern: '^[0-9u]{4}$',
    fuzzyMergeProperty: "date",
    fuzzPattern: "[^0-9]+",
    magicValueParts: ["[nu]": "[0-9nuxNUX?-]"]
)

boolean checkFuzzyDate(obj, target) {
    def props =  'startYear' in obj ? ['startYear', 'endYear'] : ['year']
    return merger.checkOnlyExpectedValuesInFuzzyProperty(target, obj, props)
}

selectBySqlWhere('''
    data#>>'{@graph,1,publication}' LIKE '%"PrimaryPublication"%"Publication"%'
    OR
    data#>>'{@graph,1,publication}' LIKE '%"PrimaryPublication"%"PrimaryPublication"%'
''') { data ->
    def (record, instance, work) = data.graph

    if (!isInstanceOf(instance, 'Instance')) {
        return
    }

    List publications = instance.publication

    if (!(publications instanceof List) || publications.size() < 2) {
        return
    }

    // Remove trivial duplicates
    Set seenPublications = new HashSet()
    publications.removeAll {
        boolean justAdded = seenPublications.add(it)
        return !justAdded
    }

    if (publications.size() == 1) {
        return
    }

    // Merge PrimaryPublication into first fuzzy-date-matching Publication.
    def primary = publications[0]
    if (isInstanceOf(primary, 'PrimaryPublication')) {
        boolean removeFirst = false
        publications[1].with {
            if (checkFuzzyDate(primary, it)) {
                // If softMerge succeds, the first (primary) has been added to
                // current it, and shall be removed.
                if (data.whelk.jsonld.softMerge(primary, it)) {
                    data.scheduleSave(loud: false)
                    removeFirst = true
                    if (it.date instanceof List &&
                        it.date.size() == 1) {
                        it.date = it.date[0]
                    }
                    if (it.date instanceof String &&
                        it.date.endsWith('.') &&
                        (it.date.size() == 1 ||
                         it.date[0..-2] == it.year)) {
                        it.remove('date')
                    }
                }
            }
        }
        if (removeFirst) {
            publications.remove(0)
        }
    }
}
