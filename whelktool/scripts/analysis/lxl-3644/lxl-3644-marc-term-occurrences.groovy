import whelk.util.DocumentUtil

PrintWriter unusedProperties = getReportWriter("unused-properties.txt")
PrintWriter unusedClasses = getReportWriter("unused-classes.txt")
PrintWriter unusedEnums = getReportWriter("unused-enums.txt")

List classList = new File(scriptDir, "marc_classes.txt").readLines()
List propertyList = new File(scriptDir, "marc_properties.txt").readLines()
List enumList = Collections.synchronizedList([])

String where = """
    collection = 'definitions' 
    AND data#>'{@graph,0,inDataset}' @> '[{\"@id\": \"https://id.kb.se/dataset/enums\"}]'
"""

selectBySqlWhere(where) {
    enumList << it.graph[1]."@id"
}

Map classes = Collections.synchronizedMap(classList.collectEntries {
    [it, false]
})
Map properties = Collections.synchronizedMap(propertyList.collectEntries {
    [it, false]
})
Map enums = Collections.synchronizedMap(enumList.collectEntries {
    [it, false]
})
Map enumsPrefixed = Collections.synchronizedMap(enumList.collectEntries {
    [it.replace("https://id.kb.se/marc/", "marc:"), it]
})

selectBySqlWhere("deleted = 'false'") { data ->
    List recAndInstance = data.graph.take(2)

    DocumentUtil.traverse(recAndInstance) { value, path ->
        if (!path)
            return

        def last = path.last()

        if (properties.containsKey(last)) {
            properties[last] = true
            incrementStats("Properties", last)
        }
        if (classes.containsKey(value)) {
            classes[value] = true
            incrementStats("Classes", value)
        }
        else if (enums.containsKey(value)) {
            enums[value] = true
            incrementStats("Enums", value)
        }
        else if (enumsPrefixed.containsKey(value)) {
            enums[enumsPrefixed[value]] = true
            incrementStats("Enums", value)
        }
    }
}

properties.findResults {it.value ? null : it.key }.each {
    unusedProperties.println(it)
}
classes.findResults {it.value ? null : it.key }.each {
    unusedClasses.println(it)
}
enumsPrefixed.findResults {it.value ? null : it.key }.each {
    unusedEnums.println(it)
}