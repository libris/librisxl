package datatool.util;
import java.util.List;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * This class keeps track of a set of disjoint (non-overlapping) sets.
 * 
 */
public class DisjointSets<T> {
	/** Sets as forest of rooted trees.
	 	Pointer to parent in tree, root points to itself. */
	List<Integer> forest;
	
	/** Rank of each tree node (keeps trees balanced when merging). */
	List<Integer> ranks;
	
	/** Sets as circular linked lists (so that we can find all elements in a set).
		Pointer to the next element in the set. */
	List<Integer> sets;
	
	/** Map from set element value to index */
	Map<T, Integer> ixs;
	
	/** Map from set element index to value */
	List<T> ixToValue;
	
	public DisjointSets(int initialCapacity) {
		forest = new ArrayList<>(initialCapacity);
		ranks = new ArrayList<>(initialCapacity);
		sets = new ArrayList<>(initialCapacity);
		ixs = new HashMap<>(initialCapacity);
		ixToValue = new ArrayList<>(initialCapacity);
	}
	
	public DisjointSets() {
		this(20);
	}
	
	/**
	 * Create a new set if it doesn't already exist.
	 * 
	 * @param e initial element in set
	 */
	public void createSet(T e) {
		if (ixs.containsKey(e)) {
			return;
		}
		
		int ix = forest.size();
		ixs.put(e, ix);
		forest.add(ix);
		ranks.add(0);
		sets.add(ix);
		ixToValue.add(e);
		
		if (ix == Integer.MAX_VALUE) {
			throw new IllegalStateException("size > Integer.MAX_VALUE");
		}
	}
	
	/**
	 * Add a set, merging it with existing intersecting sets
	 * 
	 * @param set a set to be added
	 */
	public void addSet(Iterable<T> set) {
		Iterator<T> i = set.iterator();
		if(!i.hasNext()) {
			return;
		}
		
		T first = i.next();
		while (i.hasNext()) {
			mergeSets(first, i.next());
		}
	}
	
	/**
	 * Merge two sets identified by elements. 
	 * Sets will be created if they don't exist
	 * 
	 * @param a an element of the first set
	 * @param b an element of the second set
	 */
	public void mergeSets(T a, T b) {
		if (!ixs.containsKey(a)) {
			createSet(a);
		}
		if (!ixs.containsKey(b)) {
			createSet(b);
		}
		
		int ixA = ixs.get(a);
		int ixB = ixs.get(b);
		
		int rootA = root(ixA);
		int rootB = root(ixB);
		
		if (rootA == rootB) {
			return;
		}
		
		int rankA = ranks.get(rootA);
		int rankB = ranks.get(rootB);
		
		if (rankA > rankB) {
			forest.set(rootB, rootA);
		}
		else {
			forest.set(rootA, rootB);
			if(rankA == rankB) {
				ranks.set(rootB, rankB + 1);
			}
		}
		
		int link = sets.get(rootA);
		sets.set(rootA, sets.get(rootB));
		sets.set(rootB, link);
	}
	
	/**
	 * Lookup a set based on an element in the set
	 * 
	 * @param e an element in the set
	 * @return the set
	 */
	public Set<T> getSet(T e) {
		if (!ixs.containsKey(e)) {
			throw new IllegalArgumentException("No set with element: " + e);
		}
		
		Set<T> result = new HashSet<>();
		int start = sets.get(ixs.get(e));
		int node = start;
		do {
			result.add(ixToValue.get(node));
			node = sets.get(node);
		} while(node != start);
		
		return result;
	}
	
	/**
	 * Iterate over all sets
	 * 
	 * @param visitor
	 */
	public void iterateAllSets(SetVisitor<T> visitor) {
		boolean[] visited = new boolean[sets.size()];
		
		for (int ix : sets) {
			if (visited[ix]) {
				continue;
			}
			
			int start = sets.get(ix);
			int node = start;
			do {
				visited[node] = true;
				visitor.nextElement(ixToValue.get(node));
				node = sets.get(node);
			} while(node != start);
			
			visitor.closeSet();
		}
	}
	
	/**
	 * 
	 * @return a set with all sets
	 */
	public Set<Set<T>> allSets() {
		final Set<Set<T>> result = new HashSet<>();
		
		iterateAllSets(new SetVisitor<T>() {
			Set<T> current = new HashSet<>();
			
			public void closeSet() {
				result.add(current);
				current = new HashSet<>();
			}
			
			public void nextElement(T e) {
				current.add(e);
			}
		});
		
		return result;
	}
	
	private int root(int node) {
		while (node != forest.get(node)) {
			int parent = forest.get(node);
			//path splitting - point node to grandparent
			forest.set(node, forest.get(parent));
			node = parent;
		}

		return node;
	}
	
	public interface SetVisitor<T> {
		void nextElement(T e);
		void closeSet();
	}
}
