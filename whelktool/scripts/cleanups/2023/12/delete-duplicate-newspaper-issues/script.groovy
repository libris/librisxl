def duplicates = new File(scriptDir, 'duplicates.txt').readLines()

selectBySqlWhere("collection = 'hold' and data#>>'{@graph,1,itemOf,@id}' in (${duplicates.collect { "'${it}'" }.join(',')})") {
    it.scheduleDelete()
}

selectByIds(duplicates) {
    it.scheduleDelete()
}