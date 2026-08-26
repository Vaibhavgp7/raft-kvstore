package dev.raftkv.storage;

import dev.raftkv.storage.KeyHasher.Hash128;
import java.util.BitSet;

public final class BloomFilter {

    private final BitSet bits;

    /** Total number of bits. */
    private final int bitCount;

    /** k - number of bits each key sets, mainly the number of hash functions.*/
    private final int hashCount;

    private final KeyHasher hasher;

    private BloomFilter(int bitCount, int hashCount, KeyHasher hasher) {
        this.bitCount = bitCount;
        this.hashCount = hashCount;
        this.hasher = hasher;
        this.bits = new BitSet(bitCount);
    }

    /**
     * @param expectedKeys
     * @param falsePositiveRate
     */
    public static BloomFilter create(int expectedKeys, double falsePositiveRate, KeyHasher hasher) {
        if(expectedKeys <= 0) {
            throw new IllegalArgumentException("Expected keys to be greater than zero: " + expectedKeys);
        }

        if(falsePositiveRate <= 0 || falsePositiveRate >= 1) {
            throw new IllegalArgumentException("False positive rate should be between 0 and 1: " + falsePositiveRate);
        }

        double ln2= Math.log(2);
        int bitCount = (int) Math.ceil(-expectedKeys * Math.log(falsePositiveRate)/(ln2*ln2));

        /** Round up to a whole number of bytes, so sizeInBytes() is exact*/
        bitCount = ((bitCount+7)/8)*8;

        int hashCount = Math.max(1, (int)Math.round((double) bitCount / expectedKeys * ln2));

        return new BloomFilter(bitCount, hashCount, hasher);
    }

    public static BloomFilter create(int expectedKeys) {
        return create(expectedKeys, 0.01, Sha256Hasher.INSTANCE);
    }

    public void add(byte[] key){
        Hash128 hash = hasher.hash(key);
        for(int i=0; i<hashCount; i++) {
            bits.set(bitIndex(hash, i));
        }
    }

    public boolean mightContain(byte[] key){
        Hash128 hash = hasher.hash(key);
        for(int i=0; i<hashCount; i++) {
            if(!bits.get(bitIndex(hash,i))) {
                return false;
            }
        }
        return true;
    }

    private int bitIndex(Hash128 hash, int i) {
        return (int) Math.floorMod(hash.h1() + (long) i* hash.h2(), bitCount);
    }

    public int bitCount(){
        return bitCount;
    }

    public int hashCount(){
        return hashCount;
    }

    public int sizeInBytes(){
        return bitCount/8;
    }
}
