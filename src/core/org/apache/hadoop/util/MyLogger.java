package org.apache.hadoop.util;

import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class MyLogger {
	
	private static MyLogger instance;
	private static final String logFile = "/home/hdxorbas/log_raid.log";
	
	// added to avoid all the messages from hadoop internal classes
	private final boolean enabled = false;
	private String className;
	private FileWriter fileWriter;
	
	private MyLogger() {
		System.out.println("starting MyLogger, logging at " + logFile);
	}
	
	@SuppressWarnings("rawtypes")
	private MyLogger(Class c) {
		className = c.getName();
	}
	
	private MyLogger(String name) {
		className = name;
	}
	
	//double check locking implementation (faster with threads...)
	public static synchronized MyLogger getInstance() {
		if (instance == null) {
			synchronized (MyLogger.class) {
				if (instance == null)
					instance = new MyLogger();
			}
		}
		return instance;
	}
	
	@SuppressWarnings("rawtypes")
	public static synchronized MyLogger getLogger(Class c) {
		if (instance == null) {
			synchronized (MyLogger.class) {
				if (instance == null)
					instance = new MyLogger(c);
			}
		}
		return instance;
	}
	
	public static synchronized MyLogger getLogger(String name) {
		if (instance == null) {
			synchronized (MyLogger.class) {
				if (instance == null)
					instance = new MyLogger(name);
			}
		}
		return instance;
	}
	
	/**
	 * Write any message into a file log declared as source
	 * @param msg - input string with the message to be logged
	 */
	public synchronized void write(String msg) {
		if (!enabled)
			return;
		//check if the file writer is initialized
		try {
			if (fileWriter == null)
				//the true in the end declared the file in appending mode
				fileWriter = new FileWriter(logFile, true);
			//after open, write the message
			StringBuilder builder = new StringBuilder();
			SimpleDateFormat ft = new SimpleDateFormat("yyyyMMdd HH:mm:ss.SSS");
			//add date time info
			//builder.append("[" + format.format(date) + "] (" + System.nanoTime() + ") ");
			builder.append("[" + ft.format(new Date()) + "] ");
			//if the class name exists then add it
			if (className != null && className.length() > 0)
				builder.append("{" + className + "} ");
			//add the message
			builder.append(msg);
			//Console output
			System.out.println(builder.toString());
			//add the newline character
			builder.append("\n");
			//write it in the file
			fileWriter.write(builder.toString());
		} catch (IOException ioe) {
			System.err.println("Aborting writing... file couldn't be opened!!");
		} finally {
			close();
		}
	}
	
	/**
	 * Method executed at the end of the process tested
	 */
	private void close() {
		try {
			if (fileWriter != null)
				fileWriter.close();
			//forcing garbage collection for sanity
			fileWriter = null;
		} catch (IOException ioe) {
			System.err.println("Aborting writing... file couldn't be closed!!");
		}
	}

}
