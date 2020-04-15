package whelk.component

import whelk.Document

import java.sql.Connection

interface DocumentNormalizer {
    void normalize(Document doc, Connection connection)
}