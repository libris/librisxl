package se.kb.libris.whelks.api

import se.kb.libris.whelks.api.API


interface RestAPI extends API {
    def getPath()
}
