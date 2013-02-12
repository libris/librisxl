package se.kb.libris.conch

class Tools {
    /**
     * Detects the content-type of supplied data.
     * TODO: Implement properly.
     */
    static String contentType(byte[] data) {
        return "text/plain"
    }

    static String contentType(String data) {
        return contentType(data.getBytes('UTF-8'))
    }
}

