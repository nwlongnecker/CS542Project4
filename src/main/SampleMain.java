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

import relation.RelationManager;

public class SampleMain {
	private static final String countryFilePath = "country.csv";
	private static final String cityFilePath = "city.csv";

	public static void main(String[] args) throws IOException {
		Reader countryReader = new BufferedReader(new FileReader(countryFilePath));
		Reader cityReader = new BufferedReader(new FileReader(cityFilePath));
		
		RelationManager rm = new RelationManager();
		Writer finalProjectionWriter = new OutputStreamWriter(System.out);
//		Writer finalProjectionWriter2 = new OutputStreamWriter(System.out);
		
		PipedReader projectCountryReader = new PipedReader();
		PipedWriter projectCountryWriter = new PipedWriter();
		projectCountryReader.connect(projectCountryWriter);
		
		List<Integer> keepCountry = new ArrayList<Integer>();
		keepCountry.add(0); keepCountry.add(6);
		rm.project(countryReader, projectCountryWriter, keepCountry);

		PipedReader projectCityReader = new PipedReader();
		PipedWriter projectCityWriter = new PipedWriter();
		projectCityReader.connect(projectCityWriter);
		
		List<Integer> keepCity = new ArrayList<Integer>();
		keepCity.add(2); keepCity.add(4); keepCity.add(1);
		rm.project(cityReader, projectCityWriter, keepCity);

		PipedReader joinedRelationReader = new PipedReader();
		PipedWriter joinedRelationWriter = new PipedWriter();
		joinedRelationReader.connect(joinedRelationWriter);
		rm.join(projectCountryReader, projectCityReader, joinedRelationWriter, 0, 0, (value1, value2) -> value1.equals(value2));
		
		PipedReader selectRelationReader = new PipedReader();
		PipedWriter selectRelationWriter = new PipedWriter();
		selectRelationReader.connect(selectRelationWriter);
		rm.select(joinedRelationReader, finalProjectionWriter, 1, 2, (totalPop, cityPop) -> 
				Integer.parseInt(totalPop.replaceAll("\"","")) * 0.4 < Integer.parseInt(cityPop.replaceAll("\"","")));
		
//		Writer finalProjectionWriter = new OutputStreamWriter(System.out);
//		rm.project(selectRelationReader, finalProjectionWriter);
		
		rm.executeQuery();
	}
}
