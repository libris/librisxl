List ids = new File(scriptDir, 'MODIFIED.txt').readLines()

selectByIds(ids) { data ->
    data.scheduleSave(loud: true)
}
