import whelk.datatool.bulkchange.BulkChange

// TODO
println("Hello from script! " + parameters[BulkChange.Prop.matchForm.toString()])

selectByIds(['6qjj71mj2cws3nc']) { bib ->
    bib.graph[1]['indirectlyIdentifiedBy'][0]['value'] = bib.graph[1]['indirectlyIdentifiedBy'][0]['value'] + '_5'

    bib.scheduleSave()
}