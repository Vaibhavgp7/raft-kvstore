package dev.raftkv;

import dev.raftkv.common.Bytes;

public record LogEntry(long term, Bytes command) {
    public LogEntry {
        if(term<0)
            throw new IllegalArgumentException("term must not be negative: " + term);
        if(command==null)
            throw new IllegalArgumentException("command must not be null");
    }

    // noop entry used by freshly elected leader
    public static LogEntry noop(long term) {
        return new LogEntry(term, Bytes.of(new byte[0]));
    }

    public boolean isNoop(){
        return command.length()==0;
    }
}
