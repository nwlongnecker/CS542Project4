package relation;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Reader;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Represents a folder containing relations and an undo/redo log that can have updates
 * applied to it for different transactions, and can sync with the log at any time.
 */
public class Database {

	// Used to keep track of where the relation/log files are located.
	private String databaseFolderPath;
	// Used to keep track of started/committed transactions.
	private Map<Integer, Boolean> transactions = new HashMap<Integer, Boolean>();

	/**
	 * Constructor for the Database, will sync with the log file if it exists.
	 * @param databaseFolderPath The directory to use for the database.
	 * @throws IOException If the folder is not found.
	 */
	public Database(String databaseFolderPath) throws IOException {
		
		this.databaseFolderPath = databaseFolderPath;
		syncWithLog();
	}

	/**
	 * Starts a transaction with the given number.
	 * @param transactionNum ID for the transaction.
	 */
	public void startTransaction(int transactionNum) {
		
		// Make sure this transaction ID has not yet been used.
		if (!transactions.containsKey(transactionNum)) {
			transactions.put(transactionNum, false);
			// Log that the transaction started.
		}
	}

	/**
	 * Executes an update operation on the given relation for the transaction whose ID is provided.
	 * @throws InterruptedException 
	 */
	public void update(int transactionNum, String relationName, List<Integer> compareOn, Conditional<String, Boolean> conditional,
			List<Integer> updateIndices, Conditional<String, String> newValues) throws IOException, InterruptedException {
		
		// Make sure that the transaction has been started but not yet committed.
		if (transactions.containsKey(transactionNum) && !transactions.get(transactionNum)) {
			// Create a reader and writer for the operation's input/output.
			Reader fileReader = new BufferedReader(new FileReader(databaseFolderPath + "/" + relationName + ".csv"));
			Writer fileWriter = new BufferedWriter(new FileWriter(databaseFolderPath + "/" + relationName + "_update.csv"));

			// Perform the update operation.
			Operation update = new UpdateOperation(fileReader, fileWriter, compareOn, conditional, updateIndices, newValues);
			update.start();
			update.join();

			// Replace the old relation file with the new one.
			File updated = new File(databaseFolderPath + "/" + relationName + "_update.csv");
			File old = new File(databaseFolderPath + "/" + relationName + ".csv");
			old.delete();
			updated.renameTo(old);
		}
	}

	/**
	 * Commits a transaction with the given number.
	 * @param transactionNum ID for the transaction.
	 */
	public void commit(int transactionNum) {
		
		// Make sure that the transaction has been started but not yet committed.
		if (transactions.containsKey(transactionNum) && !transactions.get(transactionNum)) {
			transactions.put(transactionNum, true);
			// Log that the transaction was committed.
		}
	}

	/**
	 * Will sync the relations in this database with the log in its folder.
	 * @throws IOException If there is no directory at the databseFolderPath location.
	 */
	public void syncWithLog() throws IOException {
		
		// Apply the undo/redo logs if they exist.
		Files.walk(Paths.get(databaseFolderPath)).forEach(filePath -> {
			if (Files.isRegularFile(filePath) && filePath.endsWith(".log")) {
				try {
					List<String> lines = Files.readAllLines(filePath);
					for (String line : lines) {
						// Parse transactions and execute them.
					}

				} catch (Exception e) {
					// Do nothing.
				}
			}
		});
	}
}
