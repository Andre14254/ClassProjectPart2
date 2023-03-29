import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.StreamingMode;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.Tuple;
import CSCI485ClassProject.Cursor;

import java.util.List;

public class FoundationDBCursor implements Cursor {
    private final Transaction tx;
    private final DirectorySubspace tableSubspace;
    private final String attrName;
    private final Object attrValue;
    private final ComparisonOperator operator;
    private final StreamingMode streamingMode;

    private KeyValue[] currentResult;

    public FoundationDBCursor(Transaction tx, DirectorySubspace tableSubspace, String attrName, Object attrValue, ComparisonOperator operator, StreamingMode streamingMode) {
        this.tx = tx;
        this.tableSubspace = tableSubspace;
        this.attrName = attrName;
        this.attrValue = attrValue;
        this.operator = operator;
        this.streamingMode = streamingMode;

        // Load the first set of results when the cursor is initialized
        updateResults(null);
    }

    private void updateResults(Tuple continuation) {
        List<KeyValue> results = tx.getRange(tableSubspace.range(), streamingMode, continuation, null, 10).asList().join();
        currentResult = results.toArray(new KeyValue[0]);
    }

    @Override
    public Record getFirst() {
        if (currentResult.length == 0) {
            return null;
        }
        Tuple recordTuple = Tuple.fromBytes(currentResult[0].getValue());
        return new RecordImpl(recordTuple);
    }

    @Override
    public Record getLast() {
        if (currentResult.length == 0) {
            return null;
        }
        Tuple recordTuple = Tuple.fromBytes(currentResult[currentResult.length - 1].getValue());
        return new RecordImpl(recordTuple);
    }

    @Override
    public Record getNext() {
        if (currentResult.length == 0) {
            return null;
        }
        Tuple continuation = Tuple.fromBytes(currentResult[currentResult.length - 1].getKey());
        updateResults(continuation);
        if (currentResult.length == 0) {
            return null;
        }
        Tuple recordTuple = Tuple.fromBytes(currentResult[0].getValue());
        return new RecordImpl(recordTuple);
    }

    @Override
    public Record getPrevious() {
        if (currentResult.length == 0) {
            return null;
        }
        Tuple continuation = Tuple.fromBytes(currentResult[0].getKey()).prepend(Tuple.from(0));
        updateResults(continuation);
        if (currentResult.length == 0) {
            return null;
        }
        Tuple recordTuple = Tuple.fromBytes(currentResult[currentResult.length - 1].getValue());
        return new RecordImpl(recordTuple);
    }
}
