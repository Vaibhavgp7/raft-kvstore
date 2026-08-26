package dev.raftkv.storage;

import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public final class Sha256Hasher implements KeyHasher {
    public static final Sha256Hasher INSTANCE = new Sha256Hasher();

    @Override
    public Hash128 hash(byte[] key) {
        MessageDigest sha256;
        try{
            sha256 = MessageDigest.getInstance("SHA-256");
        }
        catch (NoSuchAlgorithmException e){
            throw new IllegalStateException("every JVM is required to provide SHA-256", e);
        }

        byte[] digest = sha256.digest(key);

        ByteBuffer buffer = ByteBuffer.wrap(digest);
        return new Hash128(buffer.getLong(), buffer.getLong());
    }

}