package test.streams;

import java.io.StringReader;
import java.io.StringWriter;
import java.io.Writer;
import java.io.IOException;
import java.util.stream.*;

import org.apache.commons.csv.CSVFormat;

public class Main {

    public static void main(String [] args) throws IOException
    {
	String testCSV = "key,pivot_column,pivot_value,keeper1,keeper2,tosser1 \n";
	testCSV += "k1, silo1, 100, keeper1, keeper2, tosser1 \n";
	testCSV += "k1, silo2, 150, keeper1, keeper2, tosser1 \n";
	testCSV += "k2, silo1, 10, keeper1, keeper2, tosser1 \n";
	testCSV += "k2, silo2, 15, keeper1, keeper2, tosser1 \n";
	testCSV += "k2, silo4, 5, keeper1, keeper2, tosser1 \n";
	
	String output = "";

	try (PivotableCSVReader reader = new PivotableCSVReader(new StringReader(testCSV), new String[]{"key"}, CSVFormat.DEFAULT.withHeader())) {
		try(StringWriter writer = new StringWriter()) {		
			StreamSupport.stream(reader.spliterator(), false)
			    .map(rowGroup -> rowGroup.pivot("pivot_column", "pivot_value", new String[]{"keeper1", "keeper2"} ))
			    .filter(maybeRow -> maybeRow.isPresent())
			    .peek(row -> System.out.println("running over row " +  row.get()))
			    .forEach(row -> writeRowToFile(row.get(), writer));
			output = writer.toString();
		    }
		
	    }
	System.out.println("Final csv is " + output);
    }
    

    private static void writeRowToFile(Row row, Writer writer) {
	try {
	    writer.write(row.toString());
	}
	catch (IOException e)  {
	    throw new RuntimeException(e);
	}
    }
}
