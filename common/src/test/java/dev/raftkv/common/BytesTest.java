package dev.raftkv.common;

import org.junit.jupiter.api.Test;
import java.util.List;
import java.util.TreeMap;
import static org.assertj.core.api.Assertions.assertThat;

class BytesTest {
    @Test
    void equalsTest() {
        assertThat(Bytes.of("user:42")).isEqualTo(Bytes.of("user:42"));
    }

    @Test
    void hashTest() {
        var map = new java.util.HashMap<Bytes, String>();
        map.put(Bytes.of("user:42"), "Vaibhav");
        assertThat(map.get(Bytes.of("user:42"))).isEqualTo("Vaibhav");
    }

    @Test
    void sortByUnsignedValue() {
        assertThat(Bytes.of(new byte[] {0x01})).isLessThan(Bytes.of(new byte[] {(byte) 0xFF}));
    }

    @Test
    void shortKeySortsFirstWhenPrefix() {
        assertThat(Bytes.of("app")).isLessThan(Bytes.of("apple"));
    }

    @Test
    void sortsInOrderInTreeMap() {
        var map= new TreeMap<Bytes, String>();
        for(String k: List.of("apple", "banana","guava", "app")) {
            map.put(Bytes.of(k), k);
        }
        assertThat(map.values()).containsExactly("app","apple","banana","guava");
    }

    @Test
    void immutableCopy() {
        byte[] source={1,2,3};
        Bytes key=Bytes.of(source);
        source[0]=7;

        assertThat(key).isEqualTo(Bytes.of(new byte[] {1,2,3}));
    }

    @Test
    void roundTrip() {
        assertThat(Bytes.of("hello").asString()).isEqualTo("hello");
    }

}
