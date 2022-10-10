package trld.tvm;

import java.util.*;
import java.io.*;

import trld.Common;
import trld.jsonld.*;


public class Cmd {
    public static void main(String[] args) {
        String vocabFile = args.length > 0 ? args[0] : null;
        Object target = args.length > 1 ? args[1] : null;
        String inFile = args.length > 2 ? args[2] : null;

        Object vocab = Common.loadJson(vocabFile);
        vocab = Expansion.expand(vocab, vocabFile);

        if (target instanceof String && new File((String) target).isFile()) {
            target = Common.loadJson((String) target);
        } else {
            Map ctx = new HashMap();
            ctx.put("@vocab", target);
            target = new HashMap();
            ((Map) target).put("@context", ctx);
        }

        Map targetMap = Mapmaker.makeTargetMap(vocab, target);
        Object data = targetMap;

        if (inFile != null) {
            Object indata = Common.loadJson(inFile);
            indata = Expansion.expand(indata, inFile);

            Object outdata = Mapper.mapTo(targetMap, indata);
            outdata = Compaction.compact(target, outdata);
            data = outdata;
        }

        System.out.println(Common.dumpJson(data, true));
    }
}
