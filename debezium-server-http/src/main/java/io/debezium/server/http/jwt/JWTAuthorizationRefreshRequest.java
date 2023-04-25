/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.http.jwt;

import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;

@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
public class JWTAuthorizationRefreshRequest {
    private String jwtRefreshToken;
    private long tokenExpiryInMinutes;
    private long refreshTokenExpiryInMinutes;

    public JWTAuthorizationRefreshRequest() {

    }

    public JWTAuthorizationRefreshRequest(String jwtRefreshToken, long tokenExpiryInMinutes, long refreshTokenExpiryInMinutes) {
        this.jwtRefreshToken = jwtRefreshToken;
        this.tokenExpiryInMinutes = tokenExpiryInMinutes;
        this.refreshTokenExpiryInMinutes = refreshTokenExpiryInMinutes;
    }

    public String getJwtRefreshToken() {
        return jwtRefreshToken;
    }

    public void setJwtRefreshToken(String jwtRefreshToken) {
        this.jwtRefreshToken = jwtRefreshToken;
    }

    public long getTokenExpiryInMinutes() {
        return tokenExpiryInMinutes;
    }

    public void setTokenExpiryInMinutes(long tokenExpiryInMinutes) {
        this.tokenExpiryInMinutes = tokenExpiryInMinutes;
    }

    public long getRefreshTokenExpiryInMinutes() {
        return refreshTokenExpiryInMinutes;
    }

    public void setRefreshTokenExpiryInMinutes(long refreshTokenExpiryInMinutes) {
        this.refreshTokenExpiryInMinutes = refreshTokenExpiryInMinutes;
    }
}
