/**
 * This file was automatically generated by the TRLD transpiler.
 * Source: trld/jsonld/testbase.py
 */
package trld.jsonld;

//import javax.annotation.Nullable;
import java.util.*;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import java.util.stream.Collectors;
import java.io.*;

import trld.Builtins;
import trld.KeyValue;

import trld.Input;
import trld.Output;
import static trld.Common.loadJson;
import static trld.jsonld.Base.*;
import static trld.jsonld.Expansion.expand;
import static trld.jsonld.Compaction.compact;
import static trld.jsonld.Flattening.flatten;
import trld.jsonld.RdfDataset;
import static trld.jsonld.Rdf.toJsonld;
import static trld.jsonld.Rdf.toRdfDataset;
import static trld.nq.Parser.load;
import static trld.nq.Serializer.serialize;

public class Testbase {
  public static final String TESTS_URL = "https://w3c.github.io/json-ld-api/tests"; // LINE: 15
}