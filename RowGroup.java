package test.streams;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Optional;
import java.util.List;
import java.util.Map;

import org.apache.commons.csv.CSVRecord;

public class RowGroup {

    private final ArrayList<CSVRecord> rows = new ArrayList<CSVRecord>();
    private final Map<String,Integer> headerMap;

    public RowGroup(Map<String,Integer> headerMap) {
	this(headerMap, null);
    }

    public RowGroup(Map<String,Integer> headerMap, CSVRecord startRow)  
    {
	this.headerMap = headerMap;
	if (startRow != null)
	    rows.add(startRow);
    }

    public void add(CSVRecord row) {
	rows.add(row);
    }

    public Optional<Row> pivot(String pivotColumn, String pivotValue, String[] nonPivotColumns){
	Optional<Row> maybePivotedRow = Optional.empty();
	List<String> finalColumns = new ArrayList(Arrays.asList(nonPivotColumns));

	for (CSVRecord row : rows) {
	    if (!maybePivotedRow.isPresent()) {
		maybePivotedRow = Optional.of(new Row(row.toMap()));
	    }
	    String columnName = row.get(pivotColumn);
	    String columnValue = row.get(pivotValue);
	    maybePivotedRow.get().put(columnName, columnValue);
	    finalColumns.add(columnName);
	}
	Optional<Row> reducedColumns = maybePivotedRow.map( pivotedRow -> pivotedRow.filterColumns(finalColumns) );
	System.out.println("finished with row: " + reducedColumns.get());
	
	return reducedColumns;
    }
}
