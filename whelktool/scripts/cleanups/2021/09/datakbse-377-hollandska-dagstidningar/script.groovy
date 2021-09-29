List ids = new File(scriptDir, 'hollandska-libris-id.txt').readLines()
Map dataset = ["@id": "https://libris.kb.se/kz30z57kht9gmjs1#it"]

selectByIds(ids) { data ->
    data.graph[0]['inDataset'] = data.graph[0]['inDataset'] ?: []

    if (!(dataset in data.graph[0]['inDataset'])) {
        data.graph[0]['inDataset'] << dataset
        data.scheduleSave()
    }
}
