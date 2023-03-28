package CSCI485ClassProject;

import com.apple.foundationdb.*;
import com.apple.foundationdb.async.AsyncIterable;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.TupleHelpers;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class RecordsImpl implements Records {

    private final Database db;

    public RecordsImpl(Database db) {
        this.db = db;
    }

    @Override
    public StatusCode insertRecord(String tableName, String[] primaryKeys, Object[] primaryKeysValues, String[] attrNames, Object[] attrValues) {
        try {
            Transaction tr = db.createTransaction();
            byte[] primaryKeyTuple = TupleHelpers.toBytes(Tuple.from(primaryKeysValues));
            byte[] valueTuple = TupleHelpers.toBytes(Tuple.from(attrValues));
            byte[] key = tableName.getBytes();
            for (int i = 0; i < primaryKeys.length; i++) {
                key = Tuple.fromBytes(key).add(primaryKeys[i]).add(primaryKeysValues[i]).pack();
            }
            tr.set(key, Tuple.fromBytes(primaryKeyTuple).add(valueTuple).pack());
            tr.commit().join();
            return StatusCode.SUCCESS;
        } catch (Exception e) {
            return StatusCode.ERROR;
        }
    }

    @Override
    public Cursor openCursor(String tableName, String attrName, Object attrValue, ComparisonOperator operator, Cursor.Mode mode, boolean isUsingIndex) {
        try {
            Transaction tr = db.createTransaction();
            byte[] startKey = tableName.getBytes();
            byte[] endKey = tableName.getBytes();
            if (attrName != null && attrValue != null) {
                startKey = Tuple.fromBytes(startKey).add(attrName).add(operator.getValue()).add(attrValue).pack();
                endKey = Tuple.fromBytes(endKey).add(attrName).add(operator.getValue()).add(attrValue).add(new byte[]{(byte) 0xFF}).pack();
            } else {
                endKey = Tuple.fromBytes(endKey).add(new byte[]{(byte) 0xFF}).pack();
            }
            AsyncIterable<KeyValue> results = tr.getRange(startKey, endKey);
            return new Cursor(tableName, tr, results, mode);
        } catch (Exception e) {
            return null;
        }
    }

    @Override
    public Cursor openCursor(String tableName, Cursor.Mode mode) {
        return openCursor(tableName, null, null, ComparisonOperator.EQUALS, mode, false);
    }

    @Override
    public Record getFirst(Cursor cursor) {
        if (cursor.hasNext()) {
            return cursor.getNextRecord();
        }
        return null;
    }

    @Override
    public Record getLast(Cursor cursor) {
        Record last = null;
        while (cursor.hasNext()) {
            last = cursor.getNextRecord();
        }
        return last;
    }

    @Override
    public Record getNext(Cursor cursor) {
        if (cursor.hasNext()) {
            return cursor.getNextRecord();
        }
        return null;
    }

  @Override
public Record getPrevious(Cursor cursor) {
    if (cursor == null) {
        return null;
    }
    if (cursor.getCurrentRecord() == null) {
        Record lastRecord = getLast(cursor);
        if (lastRecord == null) {
            return null;
        }
    } else {
        Record previousRecord = getPreviousRecord(cursor);
        if (previousRecord == null) {
            return null;
        }
    }
    return cursor.getCurrentRecord();
}

private Record getPreviousRecord(Cursor cursor) {
    byte[] currKey = cursor.getCurrentRecord().getPrimaryKeyBytes();
    Transaction tr = cursor.tr;
    AsyncIterable<KeyValue> resultIterable = tr.getRange(
            KeySelector.lastLessThan(currKey),
            KeySelector.Before(currKey)
    );
    Cursor prevCursor = new Cursor(cursor.getTableName(), tr, resultIterable, cursor.getMode());
    if (prevCursor.hasNext()) {
        return prevCursor.getNextRecord();
    } else {
        return null;
    }
}

  @Override
  public StatusCode updateRecord(Cursor cursor, String[] attrNames, Object[] attrValues) {
    Record record = cursor.getCurrentRecord();
    if (record == null) {
        return StatusCode.NOT_FOUND;
    }

    for (int i = 0; i < attrNames.length; i++) {
        String attrName = attrNames[i];
        Object attrValue = attrValues[i];
        record.setAttr(attrName, attrValue);
    }

    cursor.updateCurrentRecord(record);
    return StatusCode.OK;
  }

  @Override
public StatusCode deleteRecord(Cursor cursor) {
    cursor.deleteCurrentRecord();
    return StatusCode.SUCCESS;
}

@Override
public StatusCode commitCursor(Cursor cursor) {
    return cursor.close()
            .thenApply(v -> StatusCode.SUCCESS)
            .exceptionally(e -> StatusCode.INTERNAL_ERROR)
            .join();
}

@Override
public StatusCode abortCursor(Cursor cursor) {
    cursor.resultIterator.cancel();
    return StatusCode.SUCCESS;
}

@Override
public StatusCode deleteDataRecord(String tableName, String[] attrNames, Object[] attrValues) {
    byte[] key = Tuple.from(tableName).pack();
    for (int i = 0; i < attrNames.length; i++) {
        key = key.addAll(Tuple.from(attrNames[i], attrValues[i]).pack());
    }
    tr.clear(key);
    return StatusCode.SUCCESS;
}

}
