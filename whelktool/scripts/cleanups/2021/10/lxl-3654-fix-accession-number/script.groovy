List ids = new File(scriptDir, 'MODIFIED.txt').readLines()

selectByIds(ids) { data ->
    Map thing = data.graph[1]

    boolean modified

    asList(thing."marc:hasImmediateSourceOfAcquisitionNote").each { acq ->
        acq.identifiedBy?.each {
            if (it.'@type' == 'AccessionNumber') {
                it['value'] = it['value']['values'][0]
                modified = true
            }
        }
    }

    asList(thing.immediateAcquisition).each { acq ->
        acq.identifiedBy?.each {
            if (it.'@type' == 'AccessionNumber') {
                it['value'] = it['value']['values'][0]
                modified = true
            }
        }
    }

    if (modified)
        data.scheduleSave()
}

List asList(o) {
    return o in List ? o : o != null ? [o] : []
}
