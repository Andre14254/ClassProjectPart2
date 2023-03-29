package CSCI485ClassProject;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.Tuple;

import CSCI485ClassProject.models.ComparisonOperator;
import CSCI485ClassProject.models.Record;

public class RecordsImpl implements Records {
    private final FDBHelper fdbHelper;

    public RecordsImpl(FDBHelper fdbHelper) {
        this.fdbHelper = fdbHelper;
    }

    @Override
    public StatusCode insertRecord(String tableName, String[] primaryKeys, Object[] primaryKeysValues, String[] attrNames, Object[] attrValues) {
        Transaction tx = fdbHelper.openTransaction();

        try {
            // Get the subspace for the table
            List<String> tablePath = Arrays.asList(tableName);
            DirectorySubspace tableSubspace = fdbHelper.createOrOpenSubspace(tx, tablePath);

            // Check if the record with the same primary keys already exists
            Tuple primaryKeyTuple = Tuple.fromArray(primaryKeysValues);
            byte[] existingRecordValue = tx.get(tableSubspace.pack(primaryKeyTuple)).join();
            if (existingRecordValue != null) {
                return StatusCode.DATA_RECORD_CREATION_RECORD_ALREADY_EXISTS;
            }

            // Construct the record tuple
            Tuple recordTuple = Tuple.fromArray(attrValues);

            // Add the record to the table subspace
            tx.set(tableSubspace.pack(primaryKeyTuple), recordTuple.pack());

            // Commit the transaction
            fdbHelper.commitTransaction(tx);
            return StatusCode.SUCCESS;

        } catch (Exception e) {
            System.out.println("ERROR: Failed to insert record: " + e.getMessage());
            fdbHelper.abortTransaction(tx);
            return StatusCode.DATA_RECORD_PRIMARY_KEYS_UNMATCHED;
        }
    }

    @Override
    public Cursor openCursor(String tableName, String attrName, Object attrValue, ComparisonOperator operator, Cursor.Mode mode, boolean isUsingIndex) {
        return null;
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
