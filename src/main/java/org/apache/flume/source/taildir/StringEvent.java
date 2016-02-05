package org.apache.flume.source.taildir;


public class StringEvent {

	  /**
	   * Returns a map of name-value pairs describing the data stored in the body.
	   */
	 // public Map<String, String> getHeaders();
	  public StringEvent(byte[] body){
		  setBody(body);
	  }
	  /**
	   * Set the event headers
	   * @param headers Map of headers to replace the current headers.
	   */
	 // public void setHeaders(Map<String, String> headers);
	  private byte[] body;
	  /**
	   * Returns the raw byte array of the data contained in this event.
	   */
	  public byte[] getBody(){
		  return body;
	  }

	  /**
	   * Sets the raw byte array of the data contained in this event.
	   * @param body The data.
	   */
	  public void setBody(byte[] body){
		  if(body == null){
		      body = new byte[0];
		    }
		    this.body = body;
	  }
	  
	  @Override
	  public String toString() {
	    Integer bodyLen = null;
	    if (body != null) bodyLen = body.length;
	    return "[body.length = " + bodyLen + " ]";
	  }

	}
