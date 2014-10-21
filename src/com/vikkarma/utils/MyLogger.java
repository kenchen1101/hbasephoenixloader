package com.vikkarma.utils;

import java.util.Date;
import java.util.logging.ConsoleHandler;
import java.util.logging.FileHandler;
import java.util.logging.Formatter;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogManager;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import com.vikkarma.hbase.enums.MyLogLevel;

public class MyLogger {
	public static Logger mylogger = Logger.getLogger("MyLogger");
	private static final CommonTestProperties testProperties = CommonTestProperties.getInstance();
	private static final String logFile=testProperties.get("LOG_FILE");
	public static final MyLogLevel logLevel = CommonTestProperties.getMyLogLevel("LOG_LEVEL");

	static {
		try {
			
			mylogger.setUseParentHandlers(false);
			//mylogger.setLevel(Level.FINE);
			switch (logLevel) {
				case FINE:
					mylogger.setLevel(Level.FINE);
					break;
				case FINER:
					mylogger.setLevel(Level.FINER);
					break;
				case FINEST:
					mylogger.setLevel(Level.FINEST);
					break;
				case INFO:
					mylogger.setLevel(Level.INFO);
					break;
				case WARNING:
					mylogger.setLevel(Level.WARNING);
					break;
				case SEVERE:
					mylogger.setLevel(Level.SEVERE);
					break;
				case OFF:
					mylogger.setLevel(Level.OFF);
					break;
				default:
					mylogger.setLevel(Level.WARNING);

			}

			Formatter formatter = new Formatter() {

				@Override
				public String format(LogRecord arg0) {
					StringBuilder b = new StringBuilder();
					b.append("[");
					b.append(new Date());
					b.append(" ");
					b.append(arg0.getSourceClassName());
					b.append(" ");
					b.append(arg0.getSourceMethodName());
					b.append(" ");
					b.append(arg0.getLevel());
					b.append("] ");
					b.append(arg0.getMessage());
					b.append(System.getProperty("line.separator"));
					return b.toString();
				}

			};
			System.out.println("**********************");
			System.out.println(logFile);		
			System.out.println("**********************");
			Thread.sleep(10);
			Handler fh = new FileHandler(logFile);
			fh.setFormatter(formatter);
			mylogger.addHandler(fh);
			
			Handler ch = new ConsoleHandler();
			ch.setFormatter(formatter);
			//mylogger.addHandler(ch);

			LogManager lm = LogManager.getLogManager();
			lm.addLogger(mylogger);
		} catch (Throwable e) {
			e.printStackTrace();
		}
	}
}
