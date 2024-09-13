package examples

Map matchForm = [
        '@type': 'Electronic'
]

Map targetForm = [
        '_id': '1',
        '@type': 'Electronic',
        'instanceOf': [
                'subject': [
                        ['@id': 'https://id.kb.se/term/sao/H%C3%A4star']
                ]
        ]
]

//selectByIds(['6qjj71mj2cws3nc']) { doc ->
selectByForm(matchForm) { doc ->
    doc.modify(matchForm, targetForm)
    doc.scheduleSave()
}
