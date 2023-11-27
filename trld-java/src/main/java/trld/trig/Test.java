package trld.trig;

//import javax.annotation.Nullable;
import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import java.util.stream.Collectors;
import java.io.*;
import static java.util.AbstractMap.SimpleEntry;

import trld.platform.Input;
import trld.jsonld.RdfDataset;
import static trld.platform.Common.jsonEncodeCanonical;
import static trld.jsonld.Base.CONTEXT;
import static trld.jsonld.Base.GRAPH;
import static trld.jsonld.Base.ID;
import static trld.jsonld.Base.TYPE;
import static trld.jsonld.Base.LIST;
import static trld.jsonld.Expansion.expand;
import static trld.jsonld.Compaction.compact;
import static trld.jsonld.Flattening.flatten;
import static trld.jsonld.Rdf.toRdfDataset;
import static trld.jsonld.Rdf.toJsonld;

public class Test {

  static class TestCase {
    public String ttype;
    public String taction;
    public String tresult;
    public TestCase(String ttype, String taction, String tresult) {
      this.ttype = ttype;
      this.taction = taction;
      this.tresult = tresult;
    }
  }

  public static Iterator<TestCase> readManifest(Object manifestPath) {
    List<TestCase> iter = new ArrayList();
    Map data = (Map) Parser.parse(new Input(manifestPath.toString()));
    Map index = ((List<Map>) ((Map) data).get(GRAPH)).stream().filter((node) -> node.containsKey(ID)).collect(Collectors.toMap((node) -> node.get(ID), (node) -> node));
    List<Map> testentries = ((Map<String, Map<String, List<Map>>>) index.get("")).get("mf:entries").get(LIST);
    for (Map testentry : testentries) {
      Map tnode = (Map) index.get(testentry.get(ID));
      String ttype = (String) (tnode.containsKey("rdf:type") ? ((Map) tnode.get("rdf:type")).get(ID) : tnode.get(TYPE));
      String taction = (String) ((Map) tnode.get("mf:action")).get(ID);
      String tresult = (String) (tnode.containsKey("mf:result") ? ((Map) tnode.get("mf:result")).get(ID) : null);
      iter.add(new TestCase(ttype, taction, tresult));
    }
    return iter.iterator();
  }

  public static void runTests(String testSuiteDir) {
    int i = 0;
    int failed = 0;
    int passed = 0;
    Iterator<TestCase> manifest = readManifest(testSuiteDir + "/manifest.ttl");
    while (manifest.hasNext()) {
      TestCase tcase = manifest.next();
      String ttype = tcase.ttype;
      String taction = tcase.taction;
      String tresult = tcase.tresult;
      i += 1;
      String trigPath = testSuiteDir + "/" + taction;
      boolean negative = "rdft:TestTrigNegativeSyntax".equals(ttype);
      Input inp = new Input(trigPath.toString());
      try {
        Map result = (Map) Parser.parse(inp);
        assert result != null;
        if (negative) {
          System.out.println("SHOULD FAIL on " + trigPath + " (a " + ttype + ")");
          failed += 1;
        } else if (tresult != null) {
          String nqPath = testSuiteDir + "/" + tresult;
          Object expected = null;
          try {
            expected = trld.nq.Parser.parse(new Input(nqPath));
          } catch (RuntimeException e) {
            System.out.println("Error parsing NQuads " + nqPath);
            throw e;
          }
          String baseUri = "http://www.w3.org/2013/TriGTests/";
          Map context = (Map) result.get(CONTEXT);
          String resultrepr = datarepr(result, context, baseUri + trigPath);
          String expectedrepr = datarepr(expected, context, baseUri + nqPath);
          if (!resultrepr.equals(expectedrepr)) {
            System.out.println("FAILED COMPARISON for " + trigPath + " (" + ttype + "). Got:n");
            System.out.println("\t" + resultrepr);
            System.out.println("Expected from " + nqPath + ":");
            System.out.println("\t" + expectedrepr);
            System.out.println();
            failed += 1;
          } else {
            passed += 1;
          }
        } else {
          passed += 1;
        }
      } catch (RuntimeException e) {
        if (negative) {
          passed += 1;
        } else {
          System.out.println("FAILED on " + trigPath + " (a " + ttype + "):");
          failed += 1;
        }
      }
    }
    System.out.println("Ran " + i + " tests. Passed " + passed + ", failed " + failed);
  }

  public static String datarepr(Object data, Map context, String baseUri) {
    data = expand(data, baseUri);
    data = (Object) flatten(data);
    RdfDataset dataset = toRdfDataset(data);
    data = (Object) toJsonld(dataset);
    data = compact(context, data, "", true);
    return jsonEncodeCanonical(data);
  }

  public static void main(String[] args) {
      runTests(args[0]);
  }
}
