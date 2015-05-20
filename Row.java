package test.streams;

import java.util.Map;
import java.util.HashMap;

public class Row {

    private Map<String, String> values;
    
    public Row(Map<String, String> initialValues) {
	values = new HashMap<>(initialValues);
    }
    
    public Row filterColumns(Iterable<String> columns){
	Map<String, String> filteredCopy = new HashMap<String,String>();
	for (String column : columns) {
	    String value = values.get(column);
	    if (value == null)
		throw new RuntimeException("When filtering on column " + column + " no values were found in row " + values.toString());

	    filteredCopy.put(column, value);
	}
	return new Row(filteredCopy);
    }

    public String get(String key) {
	return values.get(key);
    }

    public void put(String key, String value) {
	values.put(key, value);
    }

    @Override
    public String toString() {
	return values.toString();
    }
}


