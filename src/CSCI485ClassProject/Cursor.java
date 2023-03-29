package CSCI485ClassProject;

public interface Cursor {
  public enum Mode {
    READ,
    READ_WRITE
  }
  
  public Record getFirst();
  
  public Record getLast();
  
  public Record getNext();
  
  public Record getPrevious();
  
  public StatusCode updateRecord(String[] attrNames, Object[] attrValues);
  
  public StatusCode deleteRecord();
  
  public StatusCode commitCursor();
  
  public StatusCode abortCursor();
}

