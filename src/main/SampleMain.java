package main;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.LinkedList;
import java.util.List;

import relation.Database;

/**
 * Sample use of the the QueryPlanner. In this case, we extract all cities whose
 * populations are greater than 40% of their entire country's population.
 */
public class SampleMain {

	public static void main(String[] args) throws IOException, InterruptedException {
		
		// Make directories for the two databases.
		new File("sampleDatabase1").mkdir();
		new File("sampleDatabase2").mkdir();
		
		// Copy the original relation files into both databases.
		Files.copy(Paths.get("originalData/country.csv"), Paths.get("sampleDatabase1/country.csv"));
		Files.copy(Paths.get("originalData/city.csv"), Paths.get("sampleDatabase1/city.csv"));
		Files.copy(Paths.get("originalData/country.csv"), Paths.get("sampleDatabase2/country.csv"));
		Files.copy(Paths.get("originalData/city.csv"), Paths.get("sampleDatabase2/city.csv"));
		
		// Track the indices we want to update in the countries relation.
		List<Integer> countryIndices = new LinkedList<Integer>();
		countryIndices.add(6);

		// Track the indices we want to update in the cities relation.
		List<Integer> cityIndices = new LinkedList<Integer>();
		cityIndices.add(4);
		
		// Create a new Database object and update the relations as requested in the project spec.
		Database db1 = new Database("sampleDatabase1");
		// Begin a transaction.
		db1.startTransaction(1);
		// Multiply every population in the countries relation by 1.02 (increase by 2%).
		db1.update(1, "country", countryIndices, (value) -> true, countryIndices, (value) -> "\"" +
				Integer.toString((int)(Integer.parseInt(value.get(0).replace("\"", "")) * 1.02)) + "\"");
		// Multiply every population in the countries relation by 1.02 (increase by 2%).
		db1.update(1, "city", cityIndices, (value) -> true, cityIndices, (value) -> "\"" +
				Integer.toString((int)(Integer.parseInt(value.get(0).replace("\"", "")) * 1.02)) + "\"");
		// Commit the transaction.
		db1.commit(1);
		
		// Create a new Database object and sync it with the first database using its undo/redo log.
		Database db2 = new Database("sampleDatabase2");
		// Copy the log file from database1 to database2.
		Files.copy(Paths.get("sampleDatabase1/db.log"), Paths.get("sampleDatabase2/db.log"));
		// This method can be used at any time to sync with the database log.
		db2.syncWithLog();
	}
}
