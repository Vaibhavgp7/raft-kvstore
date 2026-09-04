package dev.raftkv.raft;

import java.util.ArrayList;
import java.util.List;

public final class RaftLog {
    private final List<LogEntry> entries = new ArrayList<>();

    public long lastIndex(){
        return entries.size();
    }

    public long lastTerm(){
        return termAt(lastIndex());
    }

    public long termAt(long index){
        if(index==0)return 0;
        return get(index).term();
    }

    public LogEntry get(long index){
        requireInRange(index);
        return entries.get(slot(index));
    }

    private int slot(long index){
        return (int) (index - 1);
    }

    private void requireInRange(long index){
        if(index<1 || index>lastIndex()){
            throw new IndexOutOfBoundsException("Index" + index + "is out of range" + lastIndex());
        }
    }

    public boolean isEmpty(){
        return entries.isEmpty();
    }

    public long append(LogEntry entry){
        if(entry==null){
            throw new NullPointerException("entry must not be null");
        }
        entries.add(entry);
        return lastIndex();
    }

    public List<LogEntry> entriesFrom(long fromIndex){
        if(fromIndex<1){
            throw new IndexOutOfBoundsException("fromIndex must be at least 1");
        }
        if(fromIndex>lastIndex()){
            return List.of();
        }
        return List.copyOf(entries.subList(slot(fromIndex), entries.size()));
    }

    public void truncateFrom(long fromIndex){
        if(fromIndex<1){
            throw new IndexOutOfBoundsException("fromIndex must be at least 1: " + fromIndex);
        }
        if(fromIndex>lastIndex()){
            return;
        }
        entries.subList(slot(fromIndex), entries.size()).clear();
    }

    public boolean matches(long prevLogIndex, long prevLogTerm){
        if(prevLogIndex > lastIndex()){
            return false;
        }

        return termAt(prevLogIndex) == prevLogTerm;
    }

    public long appendFrom(long prevLogIndex, List<LogEntry> incoming){
        for(int i=0;i<incoming.size();i++){
            long index = prevLogIndex+1+i;
            LogEntry entry = incoming.get(i);

            if(index > lastIndex()){
                append(entry);
            }
            else if(termAt(index)!=entry.term()){
                truncateFrom(index);
                append(entry);
            }
            // else we already have this entry
        }
        return lastIndex();
    }

    public boolean isUpToDate(long candidateLastIndex, long candidateLastTerm){
        if(candidateLastTerm != lastTerm()){
            return candidateLastTerm > lastTerm();
        }
        return candidateLastIndex >= lastIndex();
    }
}
