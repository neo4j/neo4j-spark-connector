package org.neo4j.connectors.authn.keycloak;

import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;
import org.apache.http.impl.client.HttpClients;
import org.keycloak.authorization.client.Configuration;
import org.neo4j.connectors.authn.AuthenticationToken;
import org.neo4j.connectors.authn.AuthenticationTokenSupplierFactory;


public class KeycloakOIDCAuthenticationSupplierFactory implements AuthenticationTokenSupplierFactory {

    @Override
    public String getName() {
        return "keycloak";
    }

    @Override
    public Supplier<AuthenticationToken> create(String username, String password, Map<String, String> parameters) {
        var url = Objects.requireNonNull(parameters.get("authServerUrl"));
        var realm = Objects.requireNonNull(parameters.get("realm"));
        var clientId = Objects.requireNonNull(parameters.get("clientId"));
        var clientSecret = Objects.requireNonNull(parameters.get("clientSecret"));

        return new KeycloakOIDCAuthenticationSupplier(
                username,
                password,
                new Configuration(url, realm, clientId, Map.of("secret", clientSecret), HttpClients.createMinimal()));
    }

}