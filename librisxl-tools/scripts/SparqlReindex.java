import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.Properties;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hc.client5.http.config.ConnectionConfig;
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManagerBuilder;
import org.apache.hc.core5.util.TimeValue;
import org.apache.hc.core5.util.Timeout;

import whelk.Whelk;
import whelk.component.Virtuoso;
import whelk.util.PropertyLoader;

/**
 * Simple tool for SPARQL-reindexing data from whelk using files with one XL ID
 * per line as input. Both XL short ID form (`rb7qd7m8p21dc3vt`) and SPARQL IRI
 * syntax (full or relative) (`<https://libris.kb.se/rb7qd7m8p21dc3vt>` or
 * `<rb7qd7m8p21dc3vt>`) are supported. Note that the base IRI for full IDs is
 * stripped, so ID dumps can be reused across different environments.
 *
 * The tool requires two properties files to be referenced (may be the same
 * file) using the Java system property names `xl.secret.properties` and
 * `xl.sparql.properties`. The properties `sparqlCrudUrl`, `sparqlUser`, and
 * `sparqlPass` must be defined, and using a separate file for the latter
 * prevents the SparqlUpdater from starting when the whelk is initialized.
 */
class SparqlReindex {

    static Virtuoso newSparqlCrud(Whelk whelk, Properties config) {
        int poolSize = 4;
        var cm = PoolingHttpClientConnectionManagerBuilder.create()
                        .setMaxConnTotal(poolSize)
                        .setMaxConnPerRoute(poolSize)
                        .setDefaultConnectionConfig(
                                ConnectionConfig.custom()
                                        .setConnectTimeout(Timeout.ofMilliseconds(20 * 1000))
                                        .setSocketTimeout(Timeout.ofMilliseconds(10 * 1000))
                                        .setTimeToLive(TimeValue.ofMinutes(10))
                                        .build()
                        )
                        .build();
        var jsonLdContext = whelk.getJsonld().context;
        return new Virtuoso(
            jsonLdContext,
            cm,
            config.getProperty("sparqlCrudUrl"),
            config.getProperty("sparqlUser"),
            config.getProperty("sparqlPass")
        );
    }

    public static void main(String[] args) throws IOException {
        var whelk = Whelk.createLoadedCoreWhelk();
        var config = PropertyLoader.loadProperties("secret", "sparql");
        var sparqlCrud = newSparqlCrud(whelk, config);

        var argl = new LinkedList<String>(Arrays.asList(args));
        var files = new LinkedList<String>();
        var aFrom = 0;

        while (argl.size() > 0) {
            var arg = argl.removeFirst();
            if (arg.equals("--from")) {
                aFrom = Integer.parseInt(argl.removeFirst());
            } else {
                files.add(arg);
            }
        }

        final int from = aFrom;

        var counter = new AtomicInteger(0);
        for (var file : files) {
            var fpath = Paths.get(file);
            System.out.println("Reading IDs from: " + file);
            try (var linestream = Files.lines(fpath)) {
                linestream.forEachOrdered(line -> {
                    int at = counter.incrementAndGet();
                    if (at < from) {
                        return;
                    }
                    var id = line.replaceAll("^<.*?([^/]+)>$", "$1");
                    var doc = whelk.getDocument(id);
                    if (doc != null) {
                        System.out.println("[" + at + "] Insert named graph from <" + id + ">...");
                        int attempts = 0;
                        while (attempts < 3) {
                            attempts++;
                            try {
                                sparqlCrud.insertNamedGraph(doc);
                                break;
                            } catch (Exception e) {
                                System.out.println("... error:" + e + "; trying again...");
                            }
                        }
                    }
                });
            }
        }
        System.out.println("Done.");
    }
}
