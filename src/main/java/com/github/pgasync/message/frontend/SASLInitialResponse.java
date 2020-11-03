package com.github.pgasync.message.frontend;

import com.github.pgasync.message.Message;

/**
 * it is a bit weired to have a username both here and in a {@link StartupMessage}.
 * But according to the SCRAM specification in RFC5802 we have to have it :(
 */
public class SASLInitialResponse implements Message {

    private final String saslMechanism;
    private final String channelBindingType;
    private final String username;
    private final String nonce;

    public SASLInitialResponse(String saslMechanism, String channelBindingType, String username, String nonce) {
        this.saslMechanism = saslMechanism;
        this.channelBindingType = channelBindingType;
        this.username = username;
        this.nonce = nonce;
    }

    public String getUsername() {
        return username;
    }

    public String getChannelBindingType() {
        return channelBindingType;
    }

    public String getSaslMechanism() {
        return saslMechanism;
    }

    public String getNonce() {
        return nonce;
    }
}
