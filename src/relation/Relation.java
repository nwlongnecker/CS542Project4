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
//	private String[][] tuples;
	static final int BLOCK_SIZE = 20;
	private String relationDir;
	private int numBlocks;
	private String relationName;

	/**
	 * Default constructor for a relation.
	 * @param filename Path to the file containing the tuple data.
	 * @param relationName The name of the relation
	 */
	public Relation(String filename, String relationName) {
		this.relationDir = filename.substring(0, filename.lastIndexOf("."));
		this.relationName = relationName;
		File sourceFolder = new File(relationDir);
		if (!sourceFolder.exists()) {
			String relationContents = Logger.readFile(filename);
			String[] rows = relationContents.split("\r?\n");
			// Write them back as blocks
			sourceFolder.mkdir();
			int rowIndex = 0;
			StringBuilder block = new StringBuilder();
			while (rowIndex < rows.length) {
				if (rowIndex % BLOCK_SIZE == 0 && rowIndex != 0) {
					try (FileWriter fw = new FileWriter(new File(sourceFolder, "" + ((int) (rowIndex / BLOCK_SIZE) - 1)))) {
						fw.write(block.toString());
					} catch (IOException e) {
						e.printStackTrace();
					}
					block = new StringBuilder();
				}
				block.append(rows[rowIndex]);
				block.append('\n');
				rowIndex++;
			}
			try (FileWriter fw = new FileWriter(new File(sourceFolder, "" + ((int) (rowIndex / BLOCK_SIZE))))) {
				fw.write(block.toString());
			} catch (IOException e) {
				e.printStackTrace();
			}
			numBlocks = ((int) (rowIndex / BLOCK_SIZE));
		}
	}
	
	/**
	 * Writes the relation to a given file path.
	 * @param fileName Location to write the tuples to.
	 */
	public void writeRelationToFile(String fileName) {
		try (FileWriter fw = new FileWriter(new File(fileName))) {
			for (int i = 0; i <= numBlocks; i++) {
				fw.append(Logger.readFile(relationDir + "/" + i));
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
		String relationContents = Logger.readFile(relationDir + "/" + ((int) (row / BLOCK_SIZE)));
		String[] rows = relationContents.split("\n");
		String[] columns = rows[row % BLOCK_SIZE].split(",");
		columns[column] = newValue;
		StringBuilder modifiedRow = new StringBuilder();
		for (String col : columns) {
			modifiedRow.append(',');
			modifiedRow.append(col);
		}
		rows[row % BLOCK_SIZE] = modifiedRow.toString().substring(1);
		
		StringBuilder block = new StringBuilder();
		for (int i = 0; i < rows.length; i++) {
			block.append(rows[i]);
			block.append('\n');
		}
		File sourceFolder = new File(relationDir);
		try (FileWriter fw = new FileWriter(new File(sourceFolder, "" + ((int) (row / BLOCK_SIZE))))) {
			fw.write(block.toString());
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * @return the relationName
	 */
	public String getRelationName() {
		return relationName;
	}
}
