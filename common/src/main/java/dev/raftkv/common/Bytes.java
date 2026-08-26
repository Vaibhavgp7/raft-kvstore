package dev.raftkv.common;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public final class Bytes implements Comparable<Bytes> {
    private final byte[] value;
    private final int hash;

    private Bytes(byte[] value) {
        this.value = value;
        this.hash = Arrays.hashCode(value);
    }

    public static Bytes of(byte[] value) {
        return new Bytes(value.clone());
    }

    public static Bytes of(String s){
        return new Bytes(s.getBytes(StandardCharsets.UTF_8));
    }

    public byte[] toByteArray() {
        return value;
    }

    public int length() {
        return value.length;
    }

    public String asString() {
        return new String(value, StandardCharsets.UTF_8);
    }

    @Override
    public int compareTo(Bytes other) {
        return Arrays.compareUnsigned(this.value, other.value);
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof Bytes other && Arrays.equals(this.value, other.value);
    }

    @Override
    public int hashCode() {
        return hash;
    }

    @Override
    public String toString() {
        return asString();
    }

}
