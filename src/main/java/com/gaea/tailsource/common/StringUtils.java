/**
 * 
 */
package com.gaea.tailsource.common;


/**
 * @author LX
 *
 */
public class StringUtils {

	/**
	 * 
	 */
	private StringUtils() {
		// TODO Auto-generated constructor stub
	}
	
	private static final String EMPTY = " ";
	
	public static boolean isNull(String o){
		if (o == null){
			return true;
		}else if ("null".equalsIgnoreCase(o)){
			return true;
		}else if (o.trim().length() == 0){
			return true;
		}
		return false;
	}
	
	public static String join(String separator, String...array){
		if (array == null) {
            return null;
        }
		int startIndex = 0;
		int endIndex = array.length;

        // endIndex - startIndex > 0:   Len = NofStrings *(len(firstString) + len(separator))
        //           (Assuming that all Strings are roughly equally long)
        int bufSize = (endIndex - startIndex);
        if (bufSize <= 0) {
            return EMPTY;
        }

        bufSize *= ((array[startIndex] == null ? 16 : array[startIndex].toString().length())
                        + separator.length());

        StringBuffer buf = new StringBuffer(bufSize);

        for (int i = startIndex; i < endIndex; i++) {
            if (i > startIndex) {
                buf.append(separator);
            }
            if (array[i] != null) {
                buf.append(array[i]);
            }
        }
        return buf.toString();
	}
	
	public static void main(String[] args){
		position:
		for (int i = 0; i < 10; i++){
			System.out.println("I :" + i);
			for (int j = 0; j < 10; j++){
				if (j == 5){
					System.out.println("j == 5, then break for(j");
					continue position;
				}
			}
			System.out.println("Ready to exit loop.");
			return;
		}
	}
}
