package trld.trig;

import java.util.Map;

import trld.Common;
import trld.Output;

public class serialize {

    public static void main(String[] args) {
        boolean turtleOnly = false;
        boolean turtleUnion = false;
        for (String arg : args) {
            if (arg.equals("--turtle")) {
                turtleOnly = true;
                continue;
            }
            if (arg.equals("--union")) {
                turtleUnion = true;
                continue;
            }
            Object inp = Common.loadJson(arg);
            Output out = new Output(System.out);
            if (turtleOnly) {
                Serializer.serializeTurtle((Map) inp, out, null, null, turtleUnion);
            } else {
                Serializer.serialize((Map) inp, out);
            }
        }
    }
}
