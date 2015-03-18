package solution2a;

import org.apache.hadoop.io.Text;

public class LinuxMessageLogParser {
    

  private String[] emails;
  private String status; 
  
  private boolean emailReceived;
  private boolean isMessage; 
  
  public void parse(String record) {
	String regex = ".*=<.*>.*$";
    emails = record.split(regex, -1);
    isMessage = emails.length > 0 ? true : false; 
    if (record.charAt(87) == 't')
    	emailReceived = false; 
    else
      emailReceived = true;
  }
  
  public void parse(Text record) {
    parse(record.toString());
  }

  public boolean isValidAddress() {
    return false;
  }
  
  public boolean isReceived() {
    return emailReceived;
  }
  
  public boolean logEntryIsMessage() {
	    return isMessage;
	  }
  
  public String[] getEmails() {
    return emails;
  }


}