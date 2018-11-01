package se.kb.libris.util.charcomposer;

/**
 *
 * @author marma
 */
public class CharTest {
    public static void main(String args[]) throws Exception {    
        System.out.println(ComposeUtil.decompose("\u0111", false));
        System.out.println(Integer.parseInt("0a", 16));
    }
}