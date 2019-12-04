def data =
        [ "@graph": [
                [
                        "@id": "TEMPID",
                        "mainEntity" : ["@id": "TEMPID#it"]
                ],
                [
                        "@id": "TEMPID#it",
                        "@type": "Item",
                        "heldBy": "https://libris.kb.se/library/Utb1",
                ]
        ]]

def item = create(data)
def itemList = [item]
selectFromIterable(itemList, { newlyCreatedItem ->
    newlyCreatedItem.scheduleSave()
})
