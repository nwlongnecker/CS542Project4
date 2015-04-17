package relation;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Reader;
import java.io.Writer;
import java.nio.file.FileSystemException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import logger.LogRecord;
import logger.Logger;
import logger.Transaction;

/**
 * Represents a folder containing relations and an undo/redo log that can have updates
 * applied to it for different transactions, and can sync with the log at any time.
 */
public class Database {

	// Used to keep track of where the relation/log files are located.
	private String databaseFolderPath;
	// Used to keep track of started/committed transactions.
	private Map<Integer, Transaction> transactions = new HashMap<Integer, Transaction>();
	// Every database has a logger associated with it.
	private Logger logger;

	/**
	 * Constructor for the Database, will sync with the log file if it exists.
	 * @param databaseFolderPath The directory to use for the database.
	 * @throws IOException If the folder is not found.
	 */
	public Database(String databaseFolderPath) throws IOException {
		
		this.databaseFolderPath = databaseFolderPath;
		logger = new Logger(databaseFolderPath + "/" + Logger.LOG_FILE);
	}

	/**
	 * Starts a transaction with the given number.
	 * @param transactionNum ID for the transaction.
	 */
	public void startTransaction(int transactionNum) {
		
		// Make sure this transaction ID has not yet been used.
		if (!transactions.containsKey(transactionNum)) {
			transactions.put(transactionNum, new Transaction(transactionNum, logger));
		}
	}

	/**
	 * Executes an update operation on the given relation for the transaction whose ID is provided.
	 * @throws InterruptedException If any update threads are interrupted.
	 */
	public void update(int transactionNum, String relationName, List<Integer> compareOn, Conditional<String, Boolean> conditional,
			List<Integer> updateIndices, Conditional<String, String> newValues) throws IOException, InterruptedException {
		
		// Make sure that the transaction has been started but not yet committed.
		if (transactions.containsKey(transactionNum) && !transactions.get(transactionNum).isCommitted()) {
			// Create a reader and writer for the operation's input/output.
			Reader fileReader = new BufferedReader(new FileReader(databaseFolderPath + "/" + relationName + ".csv"));
			Writer fileWriter = new BufferedWriter(new FileWriter(databaseFolderPath + "/" + relationName + "_updated.csv"));

			// Perform the update operation.
			Operation update = new UpdateOperation(fileReader, fileWriter, relationName, compareOn, conditional, updateIndices, newValues);
			update.setTransaction(transactions.get(transactionNum));
			update.start();
			update.join();

			// Replace the old relation file with the new one.
			try {
				Files.move(Paths.get(databaseFolderPath + "/" + relationName + "_updated.csv"),
					Paths.get(databaseFolderPath + "/" + relationName + ".csv"), StandardCopyOption.REPLACE_EXISTING);
			} catch (FileSystemException e) {
				System.err.println("Error renaming relation file, file exists as " + relationName + "_updated.csv");
			}
		}
	}

	/**
	 * Commits a transaction with the given number.
	 * @param transactionNum ID for the transaction.
	 */
	public void commit(int transactionNum) {
		
		// Make sure that the transaction has been started but not yet committed.
		if (transactions.containsKey(transactionNum) && !transactions.get(transactionNum).isCommitted()) {
			transactions.get(transactionNum).commit();
		}
	}

	/**
	 * Will sync the relations in this database with the log in its folder.
	 * @throws IOException If there is no directory at the databseFolderPath location.
	 */
	public void syncWithLog() throws IOException {
		
		// Get the list of logged transactions from the db.log file if it exists.
		Collection<Transaction> recoveredLog = logger.recoverLog();
		// Establish a map of all the relations in this database based on the csv files in the directory.
		HashMap<String, Relation> relations = new HashMap<String, Relation>();
		for (File f : new File(databaseFolderPath).listFiles()) {
			if (f.toPath().toString().substring(f.toPath().toString().lastIndexOf(".")+1).equals("csv")) {
				Relation r = new Relation(databaseFolderPath + "/" + f.toPath().getFileName(), f.toPath().getFileName().toString().substring(0, f.toPath().getFileName().toString().lastIndexOf(".")));
				String relationName = f.toPath().getFileName().toString().substring(0, f.toPath().getFileName().toString().lastIndexOf("."));
				relations.put(relationName, r);
			}
		}
		
		// Iterate through the transactions in the log.
		for (Transaction transaction: recoveredLog) {
			if (transaction.isCommitted()) {
				// Redo the transaction.
				List<LogRecord> redoLogs = transaction.getLogRecords();
				for (LogRecord log : redoLogs) {
					if (relations.containsKey(log.getRelationName())) {
						relations.get(log.getRelationName()).doUpdate(log.getRowNumber(), log.getColumnNumber(), log.getNewValue());
					}
				}
			} else {
				// Undo the transaction.
				List<LogRecord> undoLogs = transaction.getLogRecords();
				for (LogRecord log : undoLogs) {
					if (relations.containsKey(log.getRelationName())) {
						relations.get(log.getRelationName()).doUpdate(log.getRowNumber(), log.getColumnNumber(), log.getOldValue());
					}
				}
			}
		}
		
		// Overwrite the existing relation file with the new data.
		for (String relationName : relations.keySet()) {
			relations.get(relationName).writeRelationToFile(databaseFolderPath + "/" + relationName + "_synced.csv");
			Files.move(Paths.get(databaseFolderPath + "/" + relationName + "_synced.csv"),
					Paths.get(databaseFolderPath + "/" + relationName + ".csv"), StandardCopyOption.REPLACE_EXISTING);
		}
	}
}
