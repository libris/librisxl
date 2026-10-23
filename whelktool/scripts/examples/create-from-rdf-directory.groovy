import java.nio.file.FileVisitResult
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.nio.file.SimpleFileVisitor
import java.nio.file.attribute.BasicFileAttributes

import whelk.Whelk
import whelk.datatool.DocumentItem
import static whelk.converter.RdfReader.readRdf

Map loadRdf(Whelk whelk, String rdfSourcePath) {
    var context = whelk.storage.loadDocumentByMainId(whelk.systemContextUri).data
    return new File(rdfSourcePath).withInputStream {
        readRdf(it, rdfSourcePath, context, whelk.systemContextUri, whelk.baseUri.toString())
    }
}

DocumentItem saveRecord(Map data) {
    Map record = data[GRAPH].find { it -> it[TYPE] == 'Record' }
    assert record
    def mainId = record['mainEntity'][ID]
    assert mainId.endsWith('#it')
    Map mainEntity = data[GRAPH].find { it -> it[ID] == mainId }
    assert mainEntity
    List rest = data[GRAPH].findAll { it -> !it.is(record) && !it.is(mainEntity) }

    return create(['@graph': [record, mainEntity] + rest])
}

var rdfDirPath = Paths.get(System.getProperty("rdfdir"))
var whelk = getWhelk()

var matcher = rdfDirPath.getFileSystem().getPathMatcher('glob:**/*.{rdf,ttl,jsonld}')

Files.walkFileTree(rdfDirPath, new SimpleFileVisitor<Path>() {
    @Override
    FileVisitResult visitFile(Path path, BasicFileAttributes attrs) {
        if (!matcher.matches(path)) {
            return FileVisitResult.CONTINUE
        }
        var data = loadRdf(whelk, path.toString())
        var docItem = saveRecord(data)
        selectFromIterable([docItem], { newItem ->
            newItem.scheduleSave()
        })
        return FileVisitResult.CONTINUE
    }
})
