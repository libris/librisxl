package trld.jsonld;

import java.util.*;

import trld.Builtins;
import trld.platform.Common;

import static trld.jsonld.Base.CONTEXT;
import static trld.jsonld.Base.JSONLD10;
import static trld.jsonld.Docloader.setDocumentLoader;
import static trld.jsonld.Docloader.loadAnyDocument;
import static trld.jsonld.Testbase.TESTS_URL;

public class TestRunner {

    static /*lateinit*/ String testsuiteDir;

    static void runManifest(String manifestFile) {
        report("Running test suite: " + manifestFile);

        setDocumentLoader(new LoadDocumentCallback() {
            public RemoteDocument apply(String url, LoadDocumentOptions options) {
                System.out.println("DEBUG: " + url.replace(TESTS_URL, testsuiteDir));
                return loadAnyDocument(url.replace(TESTS_URL, testsuiteDir));
            }
        });
        Map manifest = (Map) loadAnyDocument(testsuiteDir + "/" + manifestFile).document;

        int runs = 0;
        int oks = 0;
        int fails = 0;
        int errors = 0;

        for (Map tcData : (List<Map>) manifest.get("sequence")) {
            TestCase tc = new TestCase(testsuiteDir, tcData);
            if (tc.options.getOrDefault("specVersion", "").equals(JSONLD10)) {
                continue;
            }
            runs++;
            report("Running TC " + tc.testid + " - " + tc.name + ":");
            //Context.defaultProcessingMode = tc.options.getOrDefault("processingMode", JSONLD11);
            try {
                Map.Entry<Object, Object> result = tc.run();
                Object outData = result.getKey();
                Object expectData = result.getValue();
                Object outShape = datarepr(outData);
                Object expectShape = expectData != null ? datarepr(expectData) : outShape;
                if (expectData != null && expectShape.equals(outShape)) {
                    oks++;
                    report("OK");
                } else {
                    fails++;
                    reportFailure(tc, expectShape);
                    report("  Got: "+ outShape);
                }

            } catch (RuntimeException /*JsonLdError*/ e) {
                if (tc.expectedError != null) {
                    oks++;
                    report("OK? Expected " + tc.expectedError + " got " + e.toString()); // TODO: if ((tc.expectedError).equals()) else
                    //fails++;
                } else {
                    report("Got unxpected " + e.toString());
                    e.printStackTrace(System.err);
                    errors += 1;
                }
            }
            report("");
        }
        report("Ran " + runs + " test cases. Passed: " + oks + ". Failed: " + fails + ". Errors: " + errors + ".");
    }

    static void report(String msg) {
        System.err.println(msg);
    }

    static void reportFailure(TestCase tc, Object expectData) {
        report("FAIL");
        report("  With base: '" + tc.baseUri + "'");
        report("  From file: '" + tc.indocPath + "'");
        if (tc.contextPath != null)
            report("  Using context: '" + tc.contextPath + "'");
        if (tc.expectdocPath != null)
            report("  Expecting: '" + tc.expectdocPath + "'");
        if (expectData != null)
            report("  Expected: " + expectData + "");
        if (tc.expectedError != null)
            report("Expected error: " + tc.expectedError + "");
    }

    static Object datarepr(Object data) {
        if (data instanceof String)
            return String.join("\n", Builtins.sorted(Arrays.asList(((String) data).split("\\n"))));
        return data; // Common.jsonEncode(data, true);
    }

    public static void main(String[] args) {
        testsuiteDir = "file://" + System.getProperties().get("user.dir") + "/" + args[0];
        runManifest("expand-manifest.jsonld");
        runManifest("compact-manifest.jsonld");
        runManifest("flatten-manifest.jsonld");
        runManifest("fromRdf-manifest.jsonld");
        runManifest("toRdf-manifest.jsonld");
    }

}
