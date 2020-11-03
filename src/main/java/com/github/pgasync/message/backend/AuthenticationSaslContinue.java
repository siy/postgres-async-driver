package com.github.pgasync.message.backend;

import com.github.pgasync.message.Message;

public class AuthenticationSaslContinue implements Message {

    private final byte[] saslData;

    public AuthenticationSaslContinue(byte[] saslData) {
        this.saslData = saslData;
    }

    public byte[] getSaslData() {
        return saslData;
    }
}
