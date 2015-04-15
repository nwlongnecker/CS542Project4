package relation;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import logger.Logger;

/**
 * Represents a relation whose tuples are synchronized with the data in
 * a CSV file at a provided location. Used to update relations without
 * having to directly manipulate the CSV files on disk.
 */
public class Relation {
	
	// 2D array to store all tuples in the relation.
	private String[][] tuples;
	
	/**
	 * Default constructor for a relation.
	 * @param filename Path to the file containing the tuple data.
	 */
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
	
	/**
	 * Writes the relation to a given file path.
	 * @param fileName Location to write the tuples to.
	 */
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
	
	/**
	 * Updates a specific value in the relation.
	 * @param row Row of the old value.
	 * @param column Column on the old value.
	 * @param newValue What to replace the old value with.
	 */
	public void doUpdate(int row, int column, String newValue) {
		tuples[row][column] = newValue;
	}
}
