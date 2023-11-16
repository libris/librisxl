def ids = new File(System.getProperty('clusters'))
        .readLines()
        .collect { it.split('\t').collect { it.trim()} }
        .flatten()

selectByIds(ids) {
    def instance = it.graph[1]
    def work = instance.instanceOf

    if (!work || work['@id']) return

    def illContent = work.remove('illustrativeContent')

    if (illContent) {
        instance['illustrativeContent'] = (asList(instance['illustrativeContent']) + asList(illContent)).unique()

        it.scheduleSave()
    }
}