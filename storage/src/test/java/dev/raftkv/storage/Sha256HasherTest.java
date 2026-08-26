package dev.raftkv.storage;

import dev.raftkv.storage.KeyHasher.Hash128;
import org.junit.jupiter.api.Test;
import java.nio.charset.StandardCharsets;
import java.util.Set;
import java.util.HashSet;

import static org.assertj.core.api.Assertions.assertThat;

class Sha256HasherTest {
    private final KeyHasher hasher = Sha256Hasher.INSTANCE;

    private Hash128 hash (String key){
        return hasher.hash(key.getBytes(StandardCharsets.UTF_8));
    }

    @Test
    void sameKeysameHash(){
        assertThat(hash("user:42")).isEqualTo(hash("user:42"));
    }

    @Test
    void diffKeyDiffHash(){
        assertThat(hash("user:42")).isNotEqualTo(hash("user:43"));
    }

    @Test
    void theTwoHalvesDiffer(){
        Hash128 h = hash("user:42");
        assertThat(h.h1()).isNotEqualTo(h.h2());
    }

    @Test
    void handlesAnEmptyKey(){
        Hash128 h = hasher.hash(new byte[0]);
        assertThat(h).isEqualTo(hasher.hash(new byte[0]));
    }

    @Test
    void spreadSequentialKeysWithoutCollision(){
        Set<Long> seen = new HashSet<>();
        for(int i = 0; i < 100_000; i++){
            seen.add(hash("user:"+i).h1());
        }
        assertThat(seen).hasSize(100_000);
    }

    @Test
    void oneBitOfInputChangeAboutHalfOutputBits(){
        byte[] key = "user:42".getBytes(StandardCharsets.UTF_8);
        Hash128 base = hasher.hash(key);

        byte[] flipped = key.clone();
        flipped[0] ^= 1;
        Hash128 other = hasher.hash(flipped);

        int changedBits = Long.bitCount(base.h1() ^ other.h1()) + Long.bitCount(base.h2() ^ other.h2());

        assertThat(changedBits).isBetween(45,83); //ideal is 64 of 128
    }
}
