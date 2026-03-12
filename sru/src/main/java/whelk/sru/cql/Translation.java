package whelk.sru.cql;

import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import sru.whelk.cql.cqlLexer;
import sru.whelk.cql.cqlParser;

public class Translation
{
    public static String translateCqlToXlQuery(String cqlQuery) {

        // Parse (using ANTLR)
        cqlLexer lexer = new cqlLexer( CharStreams.fromString(cqlQuery) );
        CommonTokenStream tokens = new CommonTokenStream(lexer);
        cqlParser parser = new cqlParser(tokens);
        cqlParser.CqlContext cst = parser.cql();

        // Translate
        Object phase1Ast = Phase1.reduce(cst);
        String xlql = Phase2.flatten(phase1Ast);

        // CQL searches only for instances
        if (xlql.startsWith("(") && xlql.endsWith(")"))
            xlql = xlql + " AND type=Instance";
        else
            xlql = "(" + xlql + ") AND type=Instance";

        System.err.println("CQL:  " + cqlQuery);
        System.err.println("AST:  " + phase1Ast);
        System.err.println("XLQL: " + xlql);
        System.err.println("--------------");


        return null;
    }


}