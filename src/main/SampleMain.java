package main;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PipedReader;
import java.io.PipedWriter;
import java.io.Reader;
import java.io.Writer;
import java.util.ArrayList;
import java.util.List;

import relation.QueryPlanner;

/**
 * Sample use of the the QueryPlanner. In this case, we extract all cities whose
 * populations are greater than 40% of their entire country's population.
 */
public class SampleMain {
	private static final String countryFilePath = "country.csv";
	private static final String cityFilePath = "city.csv";

	public static void main(String[] args) throws IOException {
		Reader countryReader = new BufferedReader(new FileReader(countryFilePath));
		Reader cityReader = new BufferedReader(new FileReader(cityFilePath));
		
		QueryPlanner qp = new QueryPlanner();
		
		// Set up a project on the country table
		PipedReader projectCountryReader = new PipedReader();
		PipedWriter projectCountryWriter = new PipedWriter();
		projectCountryReader.connect(projectCountryWriter);
		
		List<Integer> keepCountry = new ArrayList<Integer>();
		keepCountry.add(0); keepCountry.add(6);
		qp.project(countryReader, projectCountryWriter, keepCountry);

		// Set up a project on the city table
		PipedReader projectCityReader = new PipedReader();
		PipedWriter projectCityWriter = new PipedWriter();
		projectCityReader.connect(projectCityWriter);
		
		List<Integer> keepCity = new ArrayList<Integer>();
		keepCity.add(2); keepCity.add(4); keepCity.add(1);
		qp.project(cityReader, projectCityWriter, keepCity);

		// Set up an inner join on the results of those projections
		PipedReader joinedRelationReader = new PipedReader();
		PipedWriter joinedRelationWriter = new PipedWriter();
		joinedRelationReader.connect(joinedRelationWriter);
		qp.join(projectCountryReader, projectCityReader, joinedRelationWriter, 0, 0, (value1, value2) -> value1.equals(value2));
		
		// Set up a select on the joined relation
		PipedReader selectRelationReader = new PipedReader();
		PipedWriter selectRelationWriter = new PipedWriter();
		selectRelationReader.connect(selectRelationWriter);
		qp.select(joinedRelationReader, selectRelationWriter, 1, 2, (totalPop, cityPop) -> 
				Integer.parseInt(totalPop.replaceAll("\"","")) * 0.4 <= Integer.parseInt(cityPop.replaceAll("\"","")));
		
		// Project the final result
		Writer finalProjectionWriter = new OutputStreamWriter(System.out);
		List<Integer> keepFinal = new ArrayList<Integer>();
		keepFinal.add(3);
		qp.project(selectRelationReader, finalProjectionWriter, keepFinal);
		
		// Run the query
		qp.executeQuery();
	}
}
