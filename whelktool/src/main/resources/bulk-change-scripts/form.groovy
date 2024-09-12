import static whelk.datatool.bulkchange.BulkChange.Prop.matchForm

// TODO
println("Hello from script! " + parameters.get(matchForm))

selectByIds(['6qjj71mj2cws3nc']) { bib ->
    bib.graph[1]['indirectlyIdentifiedBy'][0]['value'] = bib.graph[1]['indirectlyIdentifiedBy'][0]['value'] + '_5'

    bib.scheduleSave(loud: isLoudAllowed)
}