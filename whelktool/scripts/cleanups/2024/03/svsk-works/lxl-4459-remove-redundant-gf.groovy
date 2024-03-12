def ids = new File(scriptDir, 'CREATED-20231212.txt').readLines()

selectByIds(ids) { docItem ->
    def work = docItem.graph[1]
    def genreForm = work.genreForm

    if (genreForm?.removeAll { it.prefLabel in ["Svensk skönlitteratur", "Utländsk skönlitteratur"] }) {
        if (genreForm.isEmpty()) {
            work.remove('genreForm')
        }
        docItem.scheduleSave()
    }
}