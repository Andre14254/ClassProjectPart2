package CSCI485ClassProject;

public class Cursor {
  public enum Mode {
    READ,
    READ_WRITE
  }
  
  private final String tableName;
  private final RecordStore recordStore;
  private RecordCursor<FDBRecord> recordCursor;
  private boolean isDirty = false;

  public Cursor(String tableName, RecordStore recordStore, RecordCursor<FDBRecord> recordCursor) {
    this.tableName = tableName;
    this.recordStore = recordStore;
    this.recordCursor = recordCursor;
  }

  public String getTableName() {
    return tableName;
  }

  public RecordStore getRecordStore() {
    return recordStore;
  }

  public RecordCursor<FDBRecord> getRecordCursor() {
    return recordCursor;
  }

  public boolean isDirty() {
    return isDirty;
  }

  public void setDirty(boolean dirty) {
    isDirty = dirty;
  }
}
