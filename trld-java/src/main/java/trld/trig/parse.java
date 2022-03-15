package trld.trig;

import java.util.ArrayList;
import java.util.List;

import trld.Common;
import trld.Input;

public class parse {

    public static void main(String[] args) {
        List<String> sources = new ArrayList<>();
        for (String arg : args) {
            sources.add(arg);
        }
        for (String src : sources) {
            Input inp = new Input(src);
            Object result = Parser.parse(inp);
            System.out.println(Common.dumpJson(result, true));
        }
    }
}
