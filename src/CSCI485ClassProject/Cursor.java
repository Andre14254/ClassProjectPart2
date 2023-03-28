package CSCI485ClassProject;

import com.apple.foundationdb.*;
import com.apple.foundationdb.async.AsyncIterable;
import com.apple.foundationdb.async.AsyncIterator;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.TupleHelpers;

import java.util.*;
import java.util.concurrent.CompletableFuture;

public class Cursor {
    public enum Mode {
        READ,
        READ_WRITE
    }

    private final String tableName;
    private final Transaction tr;
    private final AsyncIterable<KeyValue> resultIterable;
    private final AsyncIterator<KeyValue> resultIterator;
    private final Mode mode;

    private byte[] currKey;
    private byte[] currValue;

    public Cursor(String tableName, Transaction tr, AsyncIterable<KeyValue> resultIterable, Mode mode) {
        this.tableName = tableName;
        this.tr = tr;
        this.resultIterable = resultIterable;
        this.resultIterator = resultIterable.iterator();
        this.mode = mode;
    }

    public String getTableName() {
        return tableName;
    }

    public Mode getMode() {
        return mode;
    }

    public boolean hasNext() {
        return resultIterator.hasNext();
    }

    public Record getNextRecord() {
        KeyValue kv = resultIterator.next();
        currKey = kv.getKey();
        currValue = kv.getValue();
        return new Record(Tuple.fromBytes(currKey), Tuple.fromBytes(currValue));
    }

    public Record getCurrentRecord() {
        if (currKey == null || currValue == null) {
            return null;
        }
        return new Record(Tuple.fromBytes(currKey), Tuple.fromBytes(currValue));
    }

    public void updateCurrentRecord(Record record) {
        byte[] newKey = TupleHelpers.toBytes(record.getPrimaryKey());
        byte[] newValue = TupleHelpers.toBytes(record.getAttrs());
        tr.clear(currKey);
        tr.set(newKey, newValue);
        currKey = newKey;
        currValue = newValue;
    }

    public void deleteCurrentRecord() {
        tr.clear(currKey);
    }

    public CompletableFuture<Void> close() {
        return resultIterator.onClose().thenCompose(v -> {
            if (mode == Mode.READ_WRITE) {
                return tr.commitAsync();
            } else {
                return CompletableFuture.completedFuture(null);
            }
        });
    }
}
