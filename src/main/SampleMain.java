package main;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.LinkedList;
import java.util.List;

import relation.Database;

/**
 * Sample use of the the Database class. In this case, we create two databases with identical
 * city and country relations. We update the relations in the first database to have 2% increases
 * in all their populations, copy its log file to the directory of the second database, and then
 * "sync" the second database (using the undo/redo log) so it receives the same 2% changes.
 */
public class SampleMain {

	public static void main(String[] args) throws IOException, InterruptedException {
		
		// Make directories for the two databases.
		new File("sampleDatabase1").mkdir();
		new File("sampleDatabase2").mkdir();
		
		// Copy the original relation files into both databases.
		Files.copy(Paths.get("originalData/country.csv"), Paths.get("sampleDatabase1/country.csv"), StandardCopyOption.REPLACE_EXISTING);
		Files.copy(Paths.get("originalData/city.csv"), Paths.get("sampleDatabase1/city.csv"), StandardCopyOption.REPLACE_EXISTING);
		Files.copy(Paths.get("originalData/country.csv"), Paths.get("sampleDatabase2/country.csv"), StandardCopyOption.REPLACE_EXISTING);
		Files.copy(Paths.get("originalData/city.csv"), Paths.get("sampleDatabase2/city.csv"), StandardCopyOption.REPLACE_EXISTING);
		
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
		Files.copy(Paths.get("sampleDatabase1/db.log"), Paths.get("sampleDatabase2/db.log"), StandardCopyOption.REPLACE_EXISTING);
		// This method can be used at any time to sync with the database log.
		db2.syncWithLog();
	}
}
