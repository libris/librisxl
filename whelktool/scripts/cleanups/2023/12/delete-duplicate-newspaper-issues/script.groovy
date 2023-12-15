def duplicates = new File(scriptDir, 'duplicates.txt').readLines()

selectByIds(duplicates) {
    it.scheduleDelete()
}