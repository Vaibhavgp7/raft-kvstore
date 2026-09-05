package dev.raftkv.raft;

import java.util.Random;

// Node's timeouts come from this. The election timeout must be random in production and reproducible in tests.
// heartbeat interval << election timeout << mean time b/w failures
public interface Timing {

    // a follower waits without hearing from a leader before campaigning
    long nextElectionTimeout();

    long heartbeatInterval();

    static Timing production() { return randomized(150, 300, 50, new Random());}

    // same behaviour with a fixed seed, so a test replays exactly
    static Timing seeded(long seed){ return randomized(150, 300, 50, new Random(seed));}

    static Timing randomized(long minMillis , long maxMillis, long hearbeatMillis, Random random){
        if(minMillis<=0 || maxMillis<minMillis){
            throw new IllegalArgumentException("need 0<minMillis<=maxMillis");
        }
        if(hearbeatMillis<=0 || hearbeatMillis>=minMillis){
            throw new IllegalArgumentException("hearbeatInterval must be under election timeout");
        }
        return new Timing() {
            @Override
            public long nextElectionTimeout() {return minMillis + random.nextLong(maxMillis-minMillis+1);}
            @Override
            public long heartbeatInterval() {return hearbeatMillis;}
        };
    }

    //fixed election timeout for tests
    static Timing fixed(long electionMillis, long heartbeatMillis){
        return new Timing() {
            @Override
            public long nextElectionTimeout() {return electionMillis;}
            @Override
            public long heartbeatInterval() {return heartbeatMillis;}
        };
    }

}
