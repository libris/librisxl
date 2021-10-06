yieldUnescaped '<!DOCTYPE html>'
html('lang': 'sv', class: 'no-js') {
    head {
        meta('name': 'viewport', 'content': 'width=device-width, initial-scale=1')
        meta('charset': 'utf-8')
        meta('property': 'og:site_name', 'content': 'Libris')
        meta('property': 'og:title', content: title)

        title("${title} | libris.kb.se")

        link('rel': 'icon', type: 'image/svg+xml', href: 'https://libris.kb.se/assets/img/favicon.svg')
        link('rel': 'alternate icon', href: 'https://libris.kb.se/assets/img/favicon.ico')
        link('rel': 'stylesheet', 'href': 'https://libris.kb.se/assets/build/libris.css')
    }

    body('vocab': 'https://id.kb.se/vocab/', 'id': 'thing') {
        div('id': 'body-blocker') {}
        div('class': 'container', 'id': 'main') {
            div {
                div {
                    div('class': 'thing-panel') {
                        div('class': 'panel-header row') {
                            div('class': 'col-md-12') {
                                h1(title)
                            }
                            div('class': 'col-md-6') {
                                h2('Andra vyer för samma resurs:')
                                ul {
                                    if (record.mainEntity['@id'].startsWith('https://id.kb.se/')) {
                                        li {
                                            a('href': record.mainEntity["@id"], 'Visa denna resurs på id.kb.se')
                                        }
                                    }
                                    record.sameAs?.each { same ->
                                        if (same['@id'].startsWith('http://libris.kb.se/bib/')) {
                                            li {
                                                a('href': same["@id"], 'Visa denna resurs på Libris Webbsök')
                                            }
                                        }
                                    }
                                    if (record['@id']) {
                                        def thing_slug = record['@id'].tokenize('/')[-1]
                                        li {
                                            a('href': "/katalogisering/${thing_slug}", 'Visa denna resurs på Libris katalogisering')
                                        }
                                    }
                                }
                            }
                            div('class': 'col-md-6') {
                                h2('Visa som:')
                                ul('class': 'inline-list') {
                                    li {
                                        a('href': "${record['@id']}/data.jsonld", 'JSON-LD')
                                    }
                                    li {
                                        a('href': "${record['@id']}/data.ttl", 'Turtle')
                                    }
                                    li {
                                        a('href': "${record['@id']}/data.rdf", 'RDF/XML')
                                    }
                                }
                            }
                        }
                    }
                }
            }
            div('class': 'col-xs-2 col-xs-push-10 col-md-1 col-md-push-11') {
                a('href': 'https://www.kb.se/') {
                    img('src': 'https://libris.kb.se/assets/img/kb_logo_black.svg')
                }
            }
        }
    }
}
