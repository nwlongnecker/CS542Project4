package main;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.PipedReader;
import java.io.PipedWriter;
import java.io.Reader;

import relation.RelationManager;

public class SampleMain {
	private static final String countryFilePath = "../../country.csv";
	private static final String cityFilePath = "../../city.csv";

	public static void main(String[] args) throws IOException {
		Reader countryReader = new BufferedReader(new FileReader(countryFilePath));
		Reader cityReader = new BufferedReader(new FileReader(cityFilePath));
		
		RelationManager rm = new RelationManager();
		
		PipedReader projectCountryReader = new PipedReader();
		projectCountryReader.connect((PipedWriter)rm.project(countryReader));
		
		PipedReader projectCityReader = new PipedReader();
		projectCityReader.connect((PipedWriter)rm.project(cityReader));
		
		PipedReader joinedRelationReader = new PipedReader();
		joinedRelationReader.connect((PipedWriter)rm.join(projectCountryReader, projectCityReader));
		
		PipedReader selectRelationReader = new PipedReader();
		selectRelationReader.connect((PipedWriter)rm.select(joinedRelationReader));
		
		PipedReader finalProjectionReader = new PipedReader();
		finalProjectionReader.connect((PipedWriter)rm.project(selectRelationReader));
	}
}
