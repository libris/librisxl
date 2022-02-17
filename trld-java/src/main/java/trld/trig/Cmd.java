package trld.trig;

import java.util.*;

import trld.Common;
import trld.Input;

import static trld.trig.Parser.parse;

public class Cmd {

    public static void main(String[] args) {
        List<String> sources = new ArrayList<>();
        for (String arg : args) {
            sources.add(arg);
        }
        for (String src : sources) {
            Input inp = new Input(src);
            Object result = parse(inp);
            System.out.println(Common.dumpJson(result, true));
        }
    }
}
