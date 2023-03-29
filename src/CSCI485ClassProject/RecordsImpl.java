package CSCI485ClassProject;

import java.util.ArrayList;
import java.util.List;

import CSCI485ClassProject.models.ComparisonOperator;
import CSCI485ClassProject.models.Record;

public class RecordsImpl implements Records {
    private final FDBHelper fdbHelper;

    public RecordsImpl(FDBHelper fdbHelper) {
        this.fdbHelper = fdbHelper;
    }

    @Override
    public StatusCode insertRecord(String tableName, String[] primaryKeys, Object[] primaryKeysValues, String[] attrNames, Object[] attrValues) {
        Transaction tx = FDBHelper.openTransaction(db);

    try {
        // Get the subspace for the table
        List<String> tablePath = Arrays.asList(tableName);
        DirectorySubspace tableSubspace = FDBHelper.createOrOpenSubspace(tx, tablePath);

        // Check if the record with the same primary keys already exists
        Tuple primaryKeyTuple = Tuple.fromArray(primaryKeysValues);
        byte[] existingRecordValue = tx.get(tableSubspace.pack(primaryKeyTuple)).join();
        if (existingRecordValue != null) {
            return StatusCode.RECORD_ALREADY_EXISTS;
        }

        // Construct the record tuple
        Tuple recordTuple = Tuple.fromArray(attrValues);

        // Add the record to the table subspace
        tx.set(tableSubspace.pack(primaryKeyTuple), recordTuple.pack());

        // Commit the transaction
        FDBHelper.commitTransaction(tx);
        return StatusCode.OK;

    } catch (FDBException e) {
        System.out.println("ERROR: Failed to insert record: " + e.getMessage());
        FDBHelper.abortTransaction(tx);
        return StatusCode.ERROR;
    }
    }

    @Override
    public Cursor openCursor(String tableName, String attrName, Object attrValue, ComparisonOperator operator, Cursor.Mode mode, boolean isUsingIndex) {
        List<String> path = new ArrayList<>();
        path.add(tableName);

        // Open directory subspace
        Transaction tx = dbHelper.openTransaction(dbHelper.initialization());
        DirectorySubspace dir = dbHelper.openSubspace(tx, path);

        // Create range for scanning records
        Range range;
        if (attrName == null || attrValue == null) {
            range = dir.range();
        } else {
            Tuple attrTuple = Tuple.from(attrValue);
            switch (operator) {
                case EQUALS:
                    range = dir.subspace(Tuple.from(attrName)).subspace(attrTuple).range();
                    break;
                case LESS_THAN:
                    range = dir.subspace(Tuple.from(attrName)).range(null, attrTuple);
                    break;
                case LESS_THAN_OR_EQUALS:
                    range = dir.subspace(Tuple.from(attrName)).range(null, attrTuple.add(1));
                    break;
                case GREATER_THAN:
                    range = dir.subspace(Tuple.from(attrName)).range(attrTuple.add(1), null);
                    break;
                case GREATER_THAN_OR_EQUALS:
                    range = dir.subspace(Tuple.from(attrName)).range(attrTuple, null);
                    break;
                default:
                    dbHelper.abortTransaction(tx);
                    throw new IllegalArgumentException("Invalid ComparisonOperator");
            }
        }

        // Scan records and create cursor
        List<Record> records = new ArrayList<>();
        List<KeyValue> kvs = tx.getRange(range).asList().join();
        for (KeyValue kv : kvs) {
            Tuple key = dir.unpack(kv.getKey());
            Tuple value = Tuple.fromBytes(kv.getValue());
            records.add(new Record(key.toArray(), value));
        }
        dbHelper.commitTransaction(tx);
        return new CursorImpl(records, mode);
    }

  @Override
  public Cursor openCursor(String tableName, Cursor.Mode mode) {
    return null;
  }

  @Override
  public Record getFirst(Cursor cursor) {
    return null;
  }

  @Override
  public Record getLast(Cursor cursor) {
    return null;
  }

  @Override
  public Record getNext(Cursor cursor) {
    return null;
  }

  @Override
  public Record getPrevious(Cursor cursor) {
    return null;
  }

  @Override
  public StatusCode updateRecord(Cursor cursor, String[] attrNames, Object[] attrValues) {
    return null;
  }

  @Override
  public StatusCode deleteRecord(Cursor cursor) {
    return null;
  }

  @Override
  public StatusCode commitCursor(Cursor cursor) {
    return null;
  }

  @Override
  public StatusCode abortCursor(Cursor cursor) {
    return null;
  }

  @Override
  public StatusCode deleteDataRecord(String tableName, String[] attrNames, Object[] attrValues) {
    return null;
  }
}
