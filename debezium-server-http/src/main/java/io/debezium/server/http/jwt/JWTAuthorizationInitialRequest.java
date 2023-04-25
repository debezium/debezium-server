/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.server.http.jwt;

import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;

@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
public class JWTAuthorizationInitialRequest {
    private String username;
    private String password;
    private long tokenExpiryInMinutes;
    private long refreshTokenExpiryInMinutes;

    public JWTAuthorizationInitialRequest() {

    }

    public JWTAuthorizationInitialRequest(String username, String password, long tokenExpiryInMinutes, long refreshTokenExpiryInMinutes) {
        this.username = username;
        this.password = password;
        this.tokenExpiryInMinutes = tokenExpiryInMinutes;
        this.refreshTokenExpiryInMinutes = refreshTokenExpiryInMinutes;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
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
