package main;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import relation.Database;

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
		db.startTransaction(1);
		db.update(1, "country", countryIndices, (value) -> true, countryIndices, (value) -> "\"" + Integer.toString((int)(Integer.parseInt(value.get(0).replace("\"", "")) * 1.02)) + "\"");
		db.update(1, "city", cityIndices, (value) -> true, cityIndices, (value) -> "\"" + Integer.toString((int)(Integer.parseInt(value.get(0).replace("\"", "")) * 1.02)) + "\"");
		db.commit(1);
	}
}
