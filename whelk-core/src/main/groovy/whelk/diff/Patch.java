package whelk.diff;

import whelk.Document;

import java.io.IOException;
import java.util.*;

import static whelk.util.Jackson.mapper;

public class Patch {

    public static Map patch(Map base, String rfc6902patch) throws IOException {
        List patch = mapper.readValue(rfc6902patch, List.class);
        return patch(base, patch);
    }

    public static Map patch(Map base, List patch) {
        Map data = (Map) Document.deepCopy(base);
        for (Object o : patch) {
            Map op = (Map) o;
            String operation = (String) op.get("op");

            switch (operation) {
                case "add": {
                    String path = (String) op.get("path");
                    Object value = op.get("value");
                    if (path.equals("")) { // whole-document add
                        if (value instanceof Map)
                            return (Map) value;
                        return null;
                    }
                    if (!setAtRFC6901(data, value, path))
                        return null;
                    break;
                }
                case "remove": {
                    String path = (String) op.get("path");
                    if (path.equals("")) { // whole-document remove
                        data = new HashMap();
                    } else if (!removeAtRFC6901(data, path))
                        return null;
                    break;
                }
                case "replace":{
                    String path = (String) op.get("path");
                    Object value = op.get("value");
                    if (path.equals("")) { // whole-document replace
                        if (value instanceof Map)
                            data = (Map) value;
                        else
                            return null;
                    }
                    else if (getAtRFC6901(data, path) == null || !removeAtRFC6901(data, path) || !setAtRFC6901(data, value, path)) // Value at path MUST exist
                        return null;
                    break;
                }
                case "copy": {
                    String fromPath = (String) op.get("from");
                    String toPath = (String) op.get("to");
                    Object value = getAtRFC6901(data, fromPath);
                    if (value == null)
                        return null;
                    if (toPath.equals("")) { // part to whole document copy
                        if (value instanceof Map)
                            data = (Map) value;
                        else
                            return null;
                    }
                    else if (!setAtRFC6901(data, value, toPath))
                        return null;
                    break;
                }
                case "move": {
                    String fromPath = (String) op.get("from");
                    String toPath = (String) op.get("to");
                    Object value = getAtRFC6901(data, fromPath);
                    if (value == null)
                        return null;
                    if (!removeAtRFC6901(data, fromPath))
                        return null;
                    if (!setAtRFC6901(data, value, toPath))
                        return null;
                    break;
                }
                case "test": {
                    String path = (String) op.get("path");
                    Object value = op.get("value");
                    if (!getAtRFC6901(data, path).equals(value))
                        return null;
                    break;
                }
            }
        }
        return data;
    }

    private static boolean removeAtRFC6901(Map data, String pointer) {
        if (pointer.equals(""))
            return false;
        String[] tokens = tokenizePointer(pointer);

        Object node = data;

        for (int i = 0; i < tokens.length; ++i) {
            boolean finalToken = i == tokens.length-1;
            String token = tokens[i];
            token = token.replace("~1", "/");
            token = token.replace("~0", "~");

            if (node instanceof List listNode) {
                Integer index = parseArrayIndex(token);
                if (index != null && listNode.size() >= index)
                    if (finalToken) {
                        listNode.remove(index.intValue());
                        return true;
                    }
                    else
                        node = listNode.get(index);
                else
                    return false;
            } else if (node instanceof Map mapNode) {
                if (finalToken) {
                    mapNode.remove(token);
                    return true;
                }
                else
                    node = mapNode.get(token);
            } else {
                return false;
            }
        }
        return false;
    }

    private static boolean setAtRFC6901(Map data, Object value, String pointer) {
        if (pointer.equals(""))
            return false;
        String[] tokens = tokenizePointer(pointer);

        Object node = data;

        for (int i = 0; i < tokens.length; ++i) {
            boolean finalToken = i == tokens.length-1;
            String token = tokens[i];
            token = token.replace("~1", "/");
            token = token.replace("~0", "~");

            if (node instanceof List listNode) {
                Integer index = parseArrayIndex(token);
                if (index != null && listNode.size() >= index)
                    if (finalToken) {
                        listNode.add(index, value);
                        return true;
                    }
                    else
                        node = listNode.get(index);
                else
                    return false;
            } else if (node instanceof Map mapNode) {
                if (finalToken) {
                    mapNode.put(token, value);
                    return true;
                }
                else
                    node = mapNode.get(token);
            } else {
                return false;
            }
        }
        return false;
    }

    private static Object getAtRFC6901(Map data, String pointer) {
        if (pointer.equals(""))
            return data;
        String[] tokens = tokenizePointer(pointer);

        Object node = data;

        for (int i = 0; i < tokens.length; ++i) {
            String token = tokens[i];
            token = token.replace("~1", "/");
            token = token.replace("~0", "~");

            if (node instanceof List listNode) {
                Integer index = parseArrayIndex(token);
                if (index != null && listNode.size() > index)
                    node = listNode.get(index);
                else
                    return null;
            } else if (node instanceof Map mapNode) {
                if (mapNode.containsKey(token))
                    node = mapNode.get(token);
                else
                    return null;
            } else {
                return null;
            }
        }

        return node;
    }

    private static String[] tokenizePointer(String pointer) {
        if (! (pointer.charAt(0) == '/') )
            return null;
        String[] tokens;
        if (pointer.equals("/")) { // Cant split "/" on /, it just gives empty array.
            tokens = new String[] {"", ""};
        } else {
            tokens = pointer.split("/");
        }
        // Skip the initial empty string, artifact of String.split
        return Arrays.copyOfRange(tokens, 1, tokens.length);
    }

    private static Integer parseArrayIndex(String candidate) {
        // Must be in base 10, and without leading zeroes, else invalid.
        if (candidate.charAt(0) == 0 && candidate.length() > 1)
            return null;
        try {
            return Integer.parseInt(candidate, 10);
        } catch (NumberFormatException nfe) {
            return null;
        }
    }
}
