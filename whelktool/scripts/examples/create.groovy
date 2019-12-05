def data =
        [ "@graph": [
                [
                        "@id": "TEMPID",
                        "mainEntity" : ["@id": "TEMPID#it"]
                ],
                [
                        "@id": "TEMPID#it",
                        "@type": "Item",
                        "heldBy": ["@id": "https://libris.kb.se/library/Utb1"],
                        "itemOf": ["@id": "http://kblocalhost.kb.se:5000/wf7mw1h74fkt88r#it"]
                ]
        ]]

def item = create(data)
def itemList = [item]
selectFromIterable(itemList, { newlyCreatedItem ->
    newlyCreatedItem.scheduleSave()
})
