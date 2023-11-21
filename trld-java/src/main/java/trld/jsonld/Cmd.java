package trld.jsonld;

import java.util.*;

import trld.platform.Common;


public class Cmd {
    public static void main(String[] args) {
        boolean nextIsContext = false;
        boolean useFlatten = false;
        Object contextData = null;
        int configs = 0;
        List<String> sources = new ArrayList<>();
        for (String arg : args) {
            if (arg.equals("-c")) {
                nextIsContext = true;
                configs++;
                continue;
            }
            if (arg.equals("-f")) {
                useFlatten = true;
                configs++;
                continue;
            }
            if (nextIsContext) {
                contextData = loadJson(arg);
                nextIsContext = false;
                configs++;
                continue;
            }
            sources.add(arg);
        }
        for (String src : sources) {
            if ((args.length - configs) > 1) {
                System.err.println("// File: " + src);
            }
            Object source = loadJson(src);
            try {
                Object result = Expansion.expand(source, src);
                if (contextData != null) {
                    result = Compaction.compact(contextData, result, src);
                }
                if (useFlatten) {
                    result = Flattening.flatten(result, true);
                }
                System.out.println(Common.jsonEncode(result, true));
            } catch (StackOverflowError e) {
                System.err.println("// ERROR: " + e.getClass());
            } catch (Exception e) {
                if (args.length > 1) {
                    System.err.println("// ERROR: " + e);
                    e.printStackTrace(System.err);
                }
                else throw e;
            }
        }
    }

    public static Object loadJson(String ref) {
        return (Object) Docloader.loadAnyDocument(ref).document;
    }
}
