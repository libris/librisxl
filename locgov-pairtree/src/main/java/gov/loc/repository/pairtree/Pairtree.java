package gov.loc.repository.pairtree;

import java.io.File;
import java.io.UnsupportedEncodingException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;


public class Pairtree {
	public static final String HEX_INDICATOR = "^";
	
	private Character separator = File.separatorChar;
	
	private int shortyLength = 2;
	
	public int getShortyLength() {
		return this.shortyLength;
	}
	
	public void setShortyLength(int length) {
		this.shortyLength = length;
	}
	
	public Character getSeparator() {
		return separator;
	}
	
	public void setSeparator(Character separator) {
		this.separator = separator;
	}
	
	public String mapToPPath(String id) {
		assert id != null;
		String cleanId = this.cleanId(id);
		List<String> shorties = new ArrayList<String>();
		int start = 0;
		while(start < cleanId.length()) {
			int end = start + this.shortyLength;
			if (end > cleanId.length()) end = cleanId.length();
			shorties.add(cleanId.substring(start, end));
			start = end;			
		}
		return concat(shorties.toArray(new String[] {}));
	}
	
	public String mapToPPath(String basePath, String id, String encapsulatingDirName) {
		return this.concat(basePath, this.mapToPPath(id), encapsulatingDirName);
	}

	public String mapToId(String basepath, String ppath) throws InvalidPpathException {
		String newPath = this.removeBasepath(basepath, ppath);
		return this.mapToId(newPath);
	}
	
	public String mapToId(String ppath) throws InvalidPpathException {
		String id = ppath;
		if (id.endsWith(Character.toString(this.separator))) {
			id = id.substring(0, id.length()-1);
		}
		String encapsulatingDir = this.extractEncapsulatingDirFromPpath(ppath);
		if (encapsulatingDir != null) {
			id = id.substring(0, id.length() - encapsulatingDir.length());
		}
		id = id.replace(Character.toString(this.separator), "");
		id = this.uncleanId(id);
		return id;
	}

	public String extractEncapsulatingDirFromPpath(String basepath, String ppath) throws InvalidPpathException {
		String newPath = this.removeBasepath(basepath, ppath);
		return this.extractEncapsulatingDirFromPpath(newPath);
	}
	
	public String extractEncapsulatingDirFromPpath(String ppath) throws InvalidPpathException {
		assert ppath != null;
		
		//Walk the ppath looking for first non-shorty
		String[] ppathParts = ppath.split("\\" + this.separator);
		
		//If there is only 1 part
		if (ppathParts.length == 1) {
			//If part <= shorty length then no encapsulating dir
			if (ppathParts[0].length() <= this.shortyLength) {
				return null;
			}
			//Else no ppath
			else {
				throw new InvalidPpathException(MessageFormat.format("Ppath ({0}) contains no shorties", ppath));
			}
		}

		//All parts up to next to last and last should have shorty length
		for(int i=0; i < ppathParts.length-2; i++) {
			if (ppathParts[i].length() != this.shortyLength) throw new InvalidPpathException(MessageFormat.format("Ppath ({0}) has parts of incorrect length", ppath));			
		}
		String nextToLastPart = ppathParts[ppathParts.length-2];
		String lastPart = ppathParts[ppathParts.length-1];
		//Next to last should have shorty length or less
		if (nextToLastPart.length() > this.shortyLength) {
			throw new InvalidPpathException(MessageFormat.format("Ppath ({0}) has parts of incorrect length", ppath));
		}
		//If next to last has shorty length
		if (nextToLastPart.length() == this.shortyLength) {
			//If last has length > shorty length then encapsulating dir
			if (lastPart.length() > this.shortyLength) {
				return lastPart;
			}
			//Else no encapsulating dir
			else {
				return null;
			}
		}
		//Else last is encapsulating dir
		return lastPart;
					
	}
	
	private String concat(String... paths) { 
		if (paths == null || paths.length == 0) return null;
		StringBuffer pathBuf = new StringBuffer();
		Character lastChar = null;
		for(int i=0; i < paths.length; i++) {
			if (paths[i] != null) {
				if (lastChar != null && (! this.separator.equals(lastChar))) pathBuf.append(this.separator);
				pathBuf.append(paths[i]);
				lastChar = paths[i].charAt(paths[i].length()-1);
			}
		}
		return pathBuf.toString();
	}
	
	public String removeBasepath(String basePath, String path) {
		assert basePath != null;
		assert path != null;
		String newPath = path;
		if (path.startsWith(basePath)) {
			newPath = newPath.substring(basePath.length());
			if (newPath.startsWith(Character.toString(this.separator))) newPath = newPath.substring(1);
		}
		return newPath;
	}
	
	public String cleanId(String id) {
		assert id != null;
		//First pass
		byte[] bytes;
		try {
			bytes = id.getBytes("utf-8");
		} catch (UnsupportedEncodingException e) {
			throw new RuntimeException("Error getting UTF-8 for path", e);
		}
		StringBuffer idBuf = new StringBuffer();
		for(int c=0; c < bytes.length; c++) {
			byte b = bytes[c];
			int i = (int)b & 0xff;
			if (i < 0x21 
					|| i > 0x7e 
					|| i == 0x22 
					|| i == 0x2a 
					|| i == 0x2b 
					|| i == 0x2c
					|| i == 0x3c
					|| i == 0x3d
					|| i == 0x3e
					|| i == 0x3f
					|| i == 0x5c
					|| i == 0x5e
					|| i == 0x7c
			) {
				//Encode
				idBuf.append(HEX_INDICATOR);
				idBuf.append(Integer.toHexString(i));
			} else {
				//Don't encode
				char[] chars = Character.toChars(i);
				assert chars.length == 1;
				idBuf.append(chars[0]);
			}
		}
		for(int c=0; c < idBuf.length(); c++) {
			char ch = idBuf.charAt(c);
			if (ch == '/') {
				idBuf.setCharAt(c, '=');
			} else if (ch == ':') {
				idBuf.setCharAt(c, '+');
			} else if (ch == '.') {
				idBuf.setCharAt(c, ',');
			}
		}
		return idBuf.toString();
	}
	
	public String uncleanId(String id) {
		StringBuffer idBuf = new StringBuffer();
		for(int c=0; c < id.length(); c++) {
			char ch = id.charAt(c);
			if (ch == '=') {
				idBuf.append('/');
			} else if (ch == '+') {
				idBuf.append(':');
			} else if (ch == ',') {
				idBuf.append('.');
			} else if (ch == '^') {
				//Get the next 2 chars
				String hex = id.substring(c+1, c+3);
				char[] chars = Character.toChars(Integer.parseInt(hex, 16));
				assert chars.length == 1;
				idBuf.append(chars[0]);				
				c=c+2;
			} else {
				idBuf.append(ch);
			}
		}
		return idBuf.toString();
	}
	
	public class InvalidPpathException extends Exception {
		
		private static final long serialVersionUID = 1L;

		public InvalidPpathException(String msg) {
			super(msg);
		}
	}
}
