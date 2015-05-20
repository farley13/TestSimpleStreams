package test.streams;

import java.io.Closeable;
import java.io.Reader;
import java.io.IOException;

import java.util.Map;
import java.util.Iterator;
import java.util.Optional;

import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;

public class PivotableCSVReader implements Closeable, Iterable<RowGroup> {

    private final Reader wrapped;
    private final CSVParser parser;
    private final String[] pivotColumns;

    public PivotableCSVReader(Reader wrapped, String[] pivotColumns, CSVFormat format) throws IOException {
	this.wrapped = wrapped;
	this.pivotColumns = pivotColumns;
	parser = new CSVParser(wrapped, format);
    }

    @Override
    public void close() throws IOException {
	wrapped.close();
    }

    @Override
    public Iterator<RowGroup> iterator() {
	return new PivotableCSVReaderIterator();
    }

    private class PivotableCSVReaderIterator implements Iterator<RowGroup> {

	private final Iterator<CSVRecord> parserIterator;
	private Optional<CSVRecord> pendingRow = Optional.empty();

	private PivotableCSVReaderIterator(){
	    parserIterator = parser.iterator();
	}
	
	public RowGroup next() {
	    
	    RowGroup group = pendingRow.map( row -> new RowGroup(parser.getHeaderMap(), row))
		.orElse(new RowGroup(parser.getHeaderMap()));
	    
	    boolean endOfGroup = false;
	    while(parserIterator.hasNext() && !endOfGroup) {
		CSVRecord next = parserIterator.next();  
		
		System.out.println("getting input of " + next);

		if (foundNextGroup(pendingRow, next)) {
		    endOfGroup = true;
		    System.out.println("found end of group");
		}
		else {
		    group.add(next);
		}
		pendingRow = Optional.of(next);
	    }
	    return group;
	}
	
	public boolean hasNext() {
	    return parserIterator.hasNext();
	}

	private boolean foundNextGroup(Optional<CSVRecord> maybeCurrent, CSVRecord next)
	{
	    boolean foundNext = false;
	    if (maybeCurrent.isPresent()) {
		CSVRecord current = maybeCurrent.get();
		
		for(String column : pivotColumns) {
		    if(!current.get(column).equals(next.get(column))) {
			System.out.println("column " + column + "does not match: [" + current.get(column) +  "] [" +next.get(column) + "]");
			foundNext = true;
		    }
		}
	    }
	    return foundNext;
	}
    }
}
