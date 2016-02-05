package org.apache.flume.source.taildir;

public enum Status {
	  OK(0),
	  FAILED(1),
	  ERROR(2),
	  UNKNOWN(3);

	  private final int value;

	  private Status(int value) {
	    this.value = value;
	  }

	  /**
	   * Get the integer value of this enum value, as defined in the Thrift IDL.
	   */
	  public int getValue() {
	    return value;
	  }

	  /**
	   * Find a the enum type by its integer value, as defined in the Thrift IDL.
	   * @return null if the value is not found.
	   */
	  public static Status findByValue(int value) { 
	    switch (value) {
	      case 0:
	        return OK;
	      case 1:
	        return FAILED;
	      case 2:
	        return ERROR;
	      case 3:
	        return UNKNOWN;
	      default:
	        return null;
	    }
	  }
	}
