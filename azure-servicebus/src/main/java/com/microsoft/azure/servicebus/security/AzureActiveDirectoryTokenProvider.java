package com.microsoft.azure.servicebus.security;

import java.util.concurrent.CompletableFuture;
import com.microsoft.azure.servicebus.primitives.StringUtil;

/**
 * This is a token provider that obtains tokens from Azure Active Directory. The user will be able to define their own method of obtaining tokens.
 * @since 1.2.0
 *
 */
public class AzureActiveDirectoryTokenProvider extends TokenProvider
{
    private final AuthenticationCallback authCallback;
    private final String authority;
    private final Object authCallbackState;
    
    AzureActiveDirectoryTokenProvider(AuthenticationCallback callback, String authority, Object callbackState)
    {
    	this.authCallback = callback;
    	this.authority = (StringUtil.isNullOrEmpty(authority)) ? "https://login.microsoftonline.com/common" : authority;
    	this.authCallbackState = callbackState;
    }
    
    @Override
    public CompletableFuture<SecurityToken> getSecurityTokenAsync(String audience) {
    	return this.authCallback.acquireToken(audience, this.authority, this.authCallbackState);
    }
    
    @FunctionalInterface
    public interface AuthenticationCallback
    {
    	/**
    	 * A user defined method for obtaining an access token.
    	 * @param audience The target resource that the access token will be granted for.
    	 * @param authority The resource that will validate the the access token.
    	 * @param state Parameter that may be used as part of the custom acquireToken process.
    	 * @return A valid security token.
    	 */
    	CompletableFuture<SecurityToken> acquireToken(final String audience, final String authority, final Object state);
    }
}
