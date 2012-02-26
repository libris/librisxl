package se.kb.libris.whelks.temp;

import java.net.URI;
import java.net.URISyntaxException;

public class UriTest {
    public static void main(String args[]) throws URISyntaxException {
        System.out.println(new URI("whelk:abc/123"));
        System.out.println(new URI("whelk:abc/123::history"));
    }
}
