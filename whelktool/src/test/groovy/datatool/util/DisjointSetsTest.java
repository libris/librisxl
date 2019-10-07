package datatool.util;
import static org.junit.Assert.assertEquals;

import java.util.HashSet;
import java.util.Set;

import org.junit.Test;

public class DisjointSetsTest {
	
	@Test
	public void test1 () {
		DisjointSets<String> sets = new DisjointSets<>();
		sets.addSet(set("a", "b"));
		sets.addSet(set("b", "c"));
		sets.addSet(set("c", "d"));
		sets.addSet(set("d", "e"));
		sets.addSet(set("e", "f"));
		
		Set<String> expected = set("a", "b", "c", "d", "e", "f");
		
		assertEquals(set(expected), sets.allSets());
		assertEquals(expected, sets.getSet("c"));
	}
		
	@Test
	public void test2 () {
		DisjointSets<String> sets = new DisjointSets<>();
		sets.addSet(set("4", "3", "2", "1"));
		sets.addSet(set("-", "--", "---", "----"));
		sets.addSet(set("5", "6", "7", "8", "2"));
		sets.addSet(set("a", "b", "c", "d"));
		sets.addSet(set("9", "10", "11", "12", "3"));
		sets.addSet(set("p", "q", "c", "r", "s"));
		sets.addSet(set("-----", "--", "-"));
		
		Set<Set<String>> expected = set(
				set("1", "2", "3", "4", "5", "6", "7", "8","9", "10", "11", "12"), 
				set("a", "b", "c", "d", "p", "q", "r", "s"),
				set("-", "--", "---", "----", "-----"));
		
		assertEquals(expected, sets.allSets());
		assertEquals(set("a", "b", "c", "d", "p", "q", "r", "s"), sets.getSet("d"));
	}
	
	@Test
	public void test3 () {
		DisjointSets<String> sets = new DisjointSets<>();
		sets.createSet("w");
		sets.mergeSets("w", "w");
		sets.createSet("w");
		sets.mergeSets("w", "w");
		
		assertEquals(set(set("w")), sets.allSets());
		assertEquals(set("w"), sets.getSet("w"));
	}
	
	@SafeVarargs
	final private <T> Set<T> set(T ... elements) {
		Set<T> set = new HashSet<>();
		for (T e : elements) {
			set.add(e);
		}
		return set;
	}
}
