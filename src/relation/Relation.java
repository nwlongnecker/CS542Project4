package relation;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import logger.Logger;

public class Relation {
	
	private String[][] tuples;
	
	public Relation(String filename) {
		String relationContents = Logger.readFile(filename);
		String[] rows = relationContents.split("\n");
		tuples = new String[rows.length][rows[0].length()];
		for(int i = 0; i < rows.length ; i++) {
			if (rows[i] != null && !rows[i].isEmpty()) {
				tuples[i] = rows[i].split(",");
			}
		}
	}
	
	public void writeRelationToFile(String fileName) {
		try (FileWriter fw = new FileWriter(new File(fileName))) {
			for(int i = 0; i < tuples.length; i++) {
				fw.write(tuples[i][0]);
				for(int j = 1; j < tuples[i].length; j++) {
					fw.write("," + tuples[i][j]);
				}
				fw.write("\n");
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public void doUpdate(int row, int column, String newValue) {
		tuples[row][column] = newValue;
	}
}
