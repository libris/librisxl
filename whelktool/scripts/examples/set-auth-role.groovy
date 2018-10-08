def KBV = 'https://id.kb.se/vocab/'

process { data ->
    def (record, instance, work) = data.graph

    if (!work) return
    assert work['@id'] == instance.instanceOf['@id']

    work.contribution.findAll {
        it['@type'] == 'PrimaryContribution' && !it.role
    }.each {
        it.role = [
            [('@id'): KBV + 'role/aut']
        ]
        data.scheduleSave()
    }
}
