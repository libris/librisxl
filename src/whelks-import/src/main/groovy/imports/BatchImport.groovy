package se.kb.libris.imports.batch

//import se.kb.libris.whelks.storage

interface BatchImport {
    InputStream getInputStreamFor(def dataSource)
    def store(def diskStorage)
}

interface DiskImport extends BatchImport {
    def readFromDisc(def dir)

}

interface DatabaseImport extends BatchImport {
    def readFromDb(URL url)
}

interface ApiImport extends BatchImport {
    def harvest(URL url)
}

class ApiImportImpl implements ApiImport {

    InputStream getInputStreamFor(def dataSource){
        null
    }

    def store(def diskStorage){
        null
    }

    def harvest(URL url){
        null
    }

    def printSomething(){
        println "something"
    }

    static main(args){
        ApiImportImpl apiImportImpl = new ApiImportImpl()
        apiImportImpl.printSomething()
    }
}