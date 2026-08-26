package dev.raftkv.storage;

import org.junit.jupiter.api.Test;
import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BloomFilterTest {
    private static byte[] key(String s){
        return s.getBytes(StandardCharsets.UTF_8);
    }

    @Test
    void findKeysThatWereAdded(){
        BloomFilter filter = BloomFilter.create(100);
        filter.add(key("user:42"));
        assertThat(filter.mightContain(key("user:42"))).isTrue();
    }

    @Test
    void keysThatWereNotAdded(){
        BloomFilter filter = BloomFilter.create(100);
        filter.add(key("user:42"));

        assertThat(filter.mightContain(key("user:99"))).isFalse();
    }

    @Test
    void neverFalseNegative(){
        BloomFilter filter = BloomFilter.create(10_000);
        for(int i=0; i<10_000; i++){
            filter.add(key("user:"+i));
        }
        for(int i=0; i<10_000; i++){
            assertThat(filter.mightContain(key("user:"+i))).as("key user:%d was added but reported absent", i).isTrue();
        }
    }

    @Test
    void stayNearFPR(){
        int keys= 10_000;
        BloomFilter filter = BloomFilter.create(keys,0.01,Sha256Hasher.INSTANCE);
        for(int i=0; i<keys; i++){
            filter.add(key("present:"+i));
        }
        int probes = 100_000;
        int fp=0;
        for(int i=0; i<probes; i++){
            if(filter.mightContain(key("absent:"+i))){
                fp++;
            }
        }
        double measured = (double)fp/probes;

        assertThat(measured).isLessThan(0.02);
    }

}
