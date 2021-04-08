selectByCollection('bib') { bib ->
    def (record, thing) = bib.graph
    if (thing.responsibilityStatement) {
        int numContribution = asList(thing.instanceOf?.contribution).size()
        String title = thing.hasTitle?.mainTitle ?: (thing.hasTitle ?: '') 
        println(String.format("%s\t%3s\t%s\t\t%s", bib.doc.shortId, numContribution, thing.responsibilityStatement, title))
    }
}

List asList(Object o) {
    (o ?: []).with { it instanceof List ? it : [it] }
}