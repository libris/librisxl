List ids = new File(scriptDir, "MODIFIED.txt").readLines()

selectByIds(ids) { data ->
    boolean modified

    if (data.graph[1].containsKey('part')) {
        data.graph[1]['isPartOf'].each {
            removed = it.remove('part')
            if (removed)
                modified = true
        }
    }

    if (modified)
        data.scheduleSave()
}
