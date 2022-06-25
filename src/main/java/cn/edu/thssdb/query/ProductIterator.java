package cn.edu.thssdb.query;

import cn.edu.thssdb.schema.Row;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.stream.Collectors;

public class ProductIterator {
    private final List<Iterator<Row>> tableIterators;
    private final List<List<Row>> tablesData;
    private final Integer numTables;
    protected List<Row> current = null;

    protected boolean currentUsed = true;

    public ProductIterator(List<List<Row>> data) {
        tablesData = data;
        tableIterators = tablesData.stream().map(List::iterator).collect(Collectors.toList());
        numTables = tablesData.size();
    }

    protected void fetchNext() throws Exception {
    }

    public boolean hasNext() throws Exception {
        fetchNext();
        return !currentUsed;
    }

    public List<Row> next() throws Exception {
        fetchNext();
        if (currentUsed) throw new NoSuchElementException();
        currentUsed = true;
        return current;
    }

    private boolean canInit() {
        for (var iter : tableIterators) // need all has next
            if (!iter.hasNext()) return false;
        return true;
    }

    private void init() {
        if (!canInit()) throw new NoSuchElementException();
        current = new ArrayList<>();
        for (var iter : tableIterators)
            current.add(iter.next());
    }


    protected boolean unfilteredHasNext() {
        if (current == null) // need all has next
            return canInit();
        for (var iter : tableIterators) // only need one has next
            if (iter.hasNext()) return true;
        return false;
    }

    protected List<Row> unfilteredNext() {
        if (current == null) {
            init(); // may throw NoSuchElementException
            return current;
        }
        var innermost = tableIterators.get(0);
        if (innermost.hasNext()) {
            current.set(0, innermost.next());
            return current;
        }
        // exhausted
        for (int i = 1; i < numTables; ++i) { // 尝试进位
            var iter = tableIterators.get(i);
            if (iter.hasNext()) { // 可以进位
                for (int j = 0; j < i; ++j) { // 清零
                    tableIterators.set(j, tablesData.get(j).iterator());
                    current.set(j, tableIterators.get(j).next());
                }
                current.set(i, iter.next());
                return current;
            }
        }
        // 溢出
        throw new NoSuchElementException();
    }


}
