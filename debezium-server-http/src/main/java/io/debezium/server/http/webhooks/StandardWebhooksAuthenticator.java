/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.http.webhooks;

import java.net.http.HttpRequest.Builder;
import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.time.Clock;
import java.time.Instant;
import java.util.Base64;
import java.util.UUID;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

import io.debezium.DebeziumException;
import io.debezium.annotation.VisibleForTesting;
import io.debezium.server.http.Authenticator;

public class StandardWebhooksAuthenticator implements Authenticator {
    static final String SECRET_PREFIX = "whsec_";
    static final String UNBRANDED_MSG_ID_KEY = "webhook-id";
    static final String UNBRANDED_MSG_SIGNATURE_KEY = "webhook-signature";
    static final String UNBRANDED_MSG_TIMESTAMP_KEY = "webhook-timestamp";
    private static final String HMAC_SHA256 = "HmacSHA256";

    private final Clock clock;
    private final Mac sha512Hmac;

    public StandardWebhooksAuthenticator(final String secret, Clock clock) {
        this.clock = clock;

        String sec = secret;
        if (sec.startsWith(StandardWebhooksAuthenticator.SECRET_PREFIX)) {
            sec = sec.substring(StandardWebhooksAuthenticator.SECRET_PREFIX.length());
        }

        byte[] key = Base64.getDecoder().decode(sec);

        // ensure signing secret is between 24 bytes (192 bits) and 64 bytes (512 bits)
        if (key.length < 24 || key.length > 64) {
            throw new DebeziumException("Webhook secret must be between 24 and 64 bytes");
        }

        try {
            this.sha512Hmac = Mac.getInstance(HMAC_SHA256);
            SecretKeySpec keySpec = new SecretKeySpec(key, HMAC_SHA256);
            sha512Hmac.init(keySpec);
        }
        catch (InvalidKeyException | NoSuchAlgorithmException e) {
            throw new DebeziumException("Failed to initialize HMAC-SHA256 signing algorithm", e);
        }

    }

    @Override
    public void setAuthorizationHeader(Builder httpRequestBuilder, final String bodyContent, final UUID messageId) {
        final long timestamp = Instant.now(this.clock).getEpochSecond();
        final String msgId = "msg_" + messageId;
        final String signature = sign(msgId, timestamp, bodyContent);
        httpRequestBuilder.setHeader(StandardWebhooksAuthenticator.UNBRANDED_MSG_ID_KEY, msgId);
        httpRequestBuilder.setHeader(StandardWebhooksAuthenticator.UNBRANDED_MSG_SIGNATURE_KEY, signature);
        httpRequestBuilder.setHeader(StandardWebhooksAuthenticator.UNBRANDED_MSG_TIMESTAMP_KEY, Long.toString(timestamp));
    }

    @Override
    public void authenticate() {
    }

    @VisibleForTesting
    String sign(final String msgId, final long timestamp, final String payload) {
        // https://github.com/standard-webhooks/standard-webhooks/blob/main/spec/standard-webhooks.md#signature-scheme
        final String toSign = String.format("%s.%s.%s", msgId, timestamp, payload);
        byte[] macData = sha512Hmac.doFinal(toSign.getBytes(StandardCharsets.UTF_8));
        final String signature = Base64.getEncoder().encodeToString(macData);
        return String.format("v1,%s", signature);
    }
}