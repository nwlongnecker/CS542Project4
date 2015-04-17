package main;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import relation.Database;
import relation.Relation;

/**
 * Sample use of the the QueryPlanner. In this case, we extract all cities whose
 * populations are greater than 40% of their entire country's population.
 */
public class SampleMain {

	public static void main(String[] args) throws IOException, InterruptedException {
		
		// Track the indices we want to update in the countries relation.
		List<Integer> countryIndices = new LinkedList<Integer>();
		countryIndices.add(6);

		// Track the indices we want to update in the cities relation.
		List<Integer> cityIndices = new LinkedList<Integer>();
		cityIndices.add(4);
		
		// Create a new Database object and update the relations as requested in the project spec.
		Database db = new Database("sampleDatabase");
		
		// Begin a transaction.
		db.startTransaction(1);
		// Multiply every population in the countries relation by 1.02 (increase by 2%).
		db.update(1, "country", countryIndices, (value) -> true, countryIndices, (value) -> "\"" +
				Integer.toString((int)(Integer.parseInt(value.get(0).replace("\"", "")) * 1.02)) + "\"");
		// Multiply every population in the countries relation by 1.02 (increase by 2%).
		db.update(1, "city", cityIndices, (value) -> true, cityIndices, (value) -> "\"" +
				Integer.toString((int)(Integer.parseInt(value.get(0).replace("\"", "")) * 1.02)) + "\"");
		// Commit the transaction.
		db.commit(1);
		// This method can be used at any time to sync with the database log.
		db.syncWithLog();
	}
}
