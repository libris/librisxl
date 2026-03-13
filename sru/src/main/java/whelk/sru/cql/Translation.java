package whelk.sru.cql;

import org.antlr.v4.runtime.*;
import org.antlr.v4.runtime.atn.ATNConfigSet;
import org.antlr.v4.runtime.dfa.DFA;
import sru.whelk.cql.cqlLexer;
import sru.whelk.cql.cqlParser;

import java.util.BitSet;

public class Translation
{
    public static String translateCqlToXlQuery(String cqlQuery) {

        // Parse (using ANTLR)
        cqlLexer lexer = new cqlLexer( CharStreams.fromString(cqlQuery) );
        lexer.addErrorListener(new ANTLRErrorListener() {
            @Override
            public void syntaxError(Recognizer<?, ?> recognizer, Object o, int i, int i1, String s, RecognitionException e) {
                throw new RuntimeException("ANTLR Lex of query failed: " + s);
            }

            @Override
            public void reportAmbiguity(Parser parser, DFA dfa, int i, int i1, boolean b, BitSet bitSet, ATNConfigSet atnConfigSet) {
                throw new RuntimeException("ANTLR Lex of query failed (ambiguity).");
            }

            @Override
            public void reportAttemptingFullContext(Parser parser, DFA dfa, int i, int i1, BitSet bitSet, ATNConfigSet atnConfigSet) {

            }

            @Override
            public void reportContextSensitivity(Parser parser, DFA dfa, int i, int i1, int i2, ATNConfigSet atnConfigSet) {

            }
        });
        CommonTokenStream tokens = new CommonTokenStream(lexer);
        cqlParser parser = new cqlParser(tokens);
        parser.setErrorHandler(new BailErrorStrategy());
        cqlParser.CqlContext cst = parser.cql();

        // Translate
        Object phase1Ast = Phase1.reduce(cst);
        String xlql = Phase2.flatten(phase1Ast);

        // CQL searches only for instances
        if (xlql.startsWith("(") && xlql.endsWith(")"))
            xlql = xlql + " AND type=Instance";
        else
            xlql = "(" + xlql + ") AND type=Instance";

        /*
        System.err.println("CQL:  " + cqlQuery);
        System.err.println("AST:  " + phase1Ast);
        System.err.println("XLQL: " + xlql);
        System.err.println("--------------");
        */

        return xlql;
    }


}