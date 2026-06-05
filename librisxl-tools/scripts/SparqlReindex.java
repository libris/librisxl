import java.io.IOException;
import java.util.Properties;
import java.nio.file.Files;
import java.nio.file.Paths;

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
 * The tool requires the regular Java system variable `xl.secret.properties`
 * set to the appropriate XL environment; additionally containing values for
 * `sparqlCrudUrl`, `sparqlUser`, and `sparqlPass`, plus a recommended
 * `sparqlEnabled = false` to prevent the instantiated whelk from starting a
 * SparqlUpdater.
 */
class SparqlReindex {

    static Virtuoso newSparqlCrud(Whelk whelk, Properties config) {
        int poolSize = 4;
        var cm = PoolingHttpClientConnectionManagerBuilder.create()
                        .setMaxConnTotal(poolSize)
                        .setMaxConnPerRoute(poolSize)
                        .setDefaultConnectionConfig(
                                ConnectionConfig.custom()
                                        .setConnectTimeout(Timeout.ofMilliseconds(5000))
                                        .setSocketTimeout(Timeout.ofMilliseconds(5000))
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
        var config = PropertyLoader.loadProperties("secret");
        var sparqlCrud = newSparqlCrud(whelk, config);

        for (var arg : args) {
            var fpath = Paths.get(arg);
            System.out.println("Reading IDs from: " + arg);
            try (var linestream = Files.lines(fpath)) {
                linestream.forEachOrdered(line -> {
                    var id = line.replaceAll("^<.*?([^/]+)>$", "$1");
                    var doc = whelk.getDocument(id);
                    if (doc != null) {
                        System.out.println("Insert named graph from <" + id + ">...");
                        sparqlCrud.insertNamedGraph(doc);
                    }
                });
            }
        }
        System.out.println("Done.");
    }
}
