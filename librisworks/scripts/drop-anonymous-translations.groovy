import se.kb.libris.mergeworks.Doc

new File(System.getProperty('clusters')).splitEachLine(~/[\t ]+/) { cluster ->
    List<Doc> docs = Collections.synchronizedList([])
    selectByIds(cluster) {
        docs.add(new Doc(it))
    }

    def filtered = docs.split { it.instanceData }
            .with { local, linked ->
                linked + local.findAll { Doc d -> !d.isAnonymousTranslation() }
            }

    if (filtered.size() > 1) {
        println(filtered.collect { Doc d -> d.shortId() }.join('\t'))
    }
}