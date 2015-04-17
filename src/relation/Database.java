package relation;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Reader;
import java.io.Writer;
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
	private Logger logger;

	/**
	 * Constructor for the Database, will sync with the log file if it exists.
	 * @param databaseFolderPath The directory to use for the database.
	 * @throws IOException If the folder is not found.
	 */
	public Database(String databaseFolderPath) throws IOException {
		
		this.databaseFolderPath = databaseFolderPath;
		logger = new Logger(databaseFolderPath + "/" + Logger.LOG_FILE);
//		syncWithLog();
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
			Writer fileWriter = new BufferedWriter(new FileWriter(databaseFolderPath + "/" + relationName + "_update.csv"));

			// Perform the update operation.
			Operation update = new UpdateOperation(fileReader, fileWriter, relationName, compareOn, conditional, updateIndices, newValues);
			update.setTransaction(transactions.get(transactionNum));
			update.start();
			update.join();

			// Replace the old relation file with the new one.
			File updated = new File(databaseFolderPath + "/" + relationName + "_updated.csv");
			File old = new File(databaseFolderPath + "/" + relationName + ".csv");
			if (!old.delete() || !updated.renameTo(old)) {
				System.out.println("Failed to rename file, results in _updated.csv");
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
		Collection<Transaction> recoveredLog = logger.recoverLog();
		Relation cities = new Relation(databaseFolderPath + "/city.csv", "city");
		Relation countries = new Relation(databaseFolderPath + "/country.csv", "country");
		for (Transaction transaction: recoveredLog) {
			if (transaction.isCommitted()) {
				// Redo the transaction.
				List<LogRecord> redoLogs = transaction.getLogRecords();
				for (LogRecord log : redoLogs) {
					if(log.getRelationName().equals("city")) {
						cities.doUpdate(log.getRowNumber(), log.getColumnNumber(), log.getNewValue());
					} else {
						countries.doUpdate(log.getRowNumber(), log.getColumnNumber(), log.getNewValue());
					}
				}
			} else {
				// Undo the transaction.
				List<LogRecord> undoLogs = transaction.getLogRecords();
				for (LogRecord log : undoLogs) {
					if (log.getRelationName().equals("city")) {
						cities.doUpdate(log.getRowNumber(), log.getColumnNumber(), log.getOldValue());
					} else {
						countries.doUpdate(log.getRowNumber(), log.getColumnNumber(), log.getOldValue());
					}
				}
			}
		}
		cities.writeRelationToFile(databaseFolderPath + "/newCity.csv");
		countries.writeRelationToFile(databaseFolderPath + "/newCountry.csv");
	}
	
	public String getDatabaseFolderPath() {
		return databaseFolderPath;
	}
}
