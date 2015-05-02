package org.apache.activemq.util;

import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.List;

/**
 * Used so that we can batch log messages into one log line.  Originally written
 * because region broker logging was taking long and showing up in profiler runs
 * due to needing to write to disk.
 */
public class LogBatcher {

    private List<String> messages;

    private final Logger log;

    private final int flushInterval;

    public LogBatcher(Logger log) {
        this( log, 512 );
    }

    public LogBatcher(Logger log, int flushInterval) {
        this.log = log;
        this.flushInterval = flushInterval;
        reset();
    }

    public void info( String format, Object... args ) {

        messages.add( String.format( format, args ) );

        if (messages.size() >= flushInterval) {
            flush();
        }

    }

    public void flush() {

        if(messages.size() > 0) {

            StringBuilder buff = new StringBuilder();

            for (String message : messages) {
                buff.append( message );
                buff.append( "\n" );
            }

            log.info( buff.toString() );

            reset();
        }

    }

    public void reset() {
        this.messages = new ArrayList<String>(flushInterval);

    }

}
