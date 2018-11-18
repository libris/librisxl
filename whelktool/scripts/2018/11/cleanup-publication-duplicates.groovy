import whelk.converter.marc.RestructPropertyValuesAndFlagStep

merger = new RestructPropertyValuesAndFlagStep(
    matchValuePattern: '^[0-9u]{4}$',
    fuzzyMergeProperty: "date",
    fuzzPattern: "[^0-9]+",
    magicValueParts: ["u": "[0-9nuxNUX?-]"]
)

boolean checkFuzzyDate(obj, target) {
    def props = ['year'] // TODO: or startYear, endYear
    return merger.checkOnlyExpectedValuesInFuzzyProperty(target, obj, props)
}

selectBySqlWhere('''
    data#>>'{@graph,1,publication}' LIKE '%"PrimaryPublication"%"Publication"%'
''') { data ->
    def (record, instance, work) = data.graph

    if (!isInstanceOf(instance, 'Instance')) {
        return
    }

    if (instance.publication.size() < 2) {
        return
    }

    def obj = instance.publication[0]
    boolean removeFirst = false
    instance.publication[1].with {
        if (checkFuzzyDate(obj, it)) {
            if (data.whelk.jsonld.softMerge(obj, it)) {
                data.scheduleSave(loud: false)
                removeFirst = true
            }
        }
    }
    if (removeFirst) {
        instance.publication.remove(0)
    }


}
