package datatool.util;

import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class DocumentDiff {
	private static final Comparator<Object> COMPARATOR = new Comparator<Object>() {
		@Override
		public int compare(Object o1, Object o2) {
			return o2.hashCode() - o1.hashCode();
		}
	};
	
	public boolean equals(Map<?,?> a, Map<?,?> b) {
		if (a == null || b == null || a.size() != b.size()) {
			return false;
		}
		for (Object key : a.keySet()) {
			if (!equals(a.get(key), b.get(key), key)) {
				return false;
			}
		}
		return true;
	}
	
	private boolean equals(Object a, Object b, Object key) {
		if (a == null || b == null || a.getClass() != b.getClass()) {
			return false;
		}
		else if (a instanceof Map) {
			return equals((Map<?,?>) a, (Map<?,?>) b);
		}
		else if (a instanceof List) {
			if (ordered(key)) {
				return equalsList((List<?>) a, (List<?>)b);
			}
			else {
				return equalsSet((List<?>) a, (List<?>)b);
			}
		}
		else {
			return a.equals(b);
		}
	}
	
	private boolean equalsList(List<?> a, List<?> b) {
		if (a.size() != b.size()) {
			return false;
		}
		for (int i = 0 ; i < a.size() ; i++) {
			if (!equals(a.get(i), b.get(i), null)) {
				return false;
			}
		}
		return true;
	}
	
	private boolean equalsSet(List<?> a, List<?> b) {
		if (a.size() != b.size()) {
			return false;
		}
		
		a.sort(COMPARATOR);
		b.sort(COMPARATOR);
		
		return equalsList(a, b);
	}
	
	public boolean subset(Map<?,?> a, Map<?,?> b) {
		if (a == null || b == null || a.size() > b.size()) {
			return false;
		}
		for (Object key : a.keySet()) {
			if (!subset(a.get(key), b.get(key), key)) {
				return false;
			}
		}
		return true;
	}
	
	private boolean subset(Object a, Object b, Object key) {
		if (a == null || b == null || a.getClass() != b.getClass()) {
			return false;
		}
		else if (a instanceof Map) {
			return subset((Map<?,?>) a, (Map<?,?>) b);
		}
		else if (a instanceof List) {
			if (ordered(key)) {
				return isListSubset((List<?>) a, (List<?>)b);
			}
			else {
				return isSetSubset((List<?>) a, (List<?>)b);
			}
		}
		else {
			return a.equals(b);
		}
	}
	
	private boolean isListSubset(List<?> a, List<?> b) {
		if (a.size() > b.size()) {
			return false;
		}
		for (int i = 0 ; i < a.size() ; i++) {
			if (!equals(a.get(i), b.get(i), null)) {
				return false;
			}
		}
		return true;
	}
	
	private boolean isSetSubset(List<?> a, List<?> b) {
		if (a.size() > b.size()) {
			return false;
		}
		
		Set<?> aSet = new HashSet<>(a);
		Set<?> bSet = new HashSet<>(b);
		
		// TODO: what now?
		
		return false;
	}
	
	private boolean ordered (Object key) {
		return "termComponentList".equals(key);
	}
}
