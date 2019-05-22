package com.microsoft.azure.servicebus.security;

import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;

import com.microsoft.azure.servicebus.primitives.StringUtil;

/**
 * This is a token provider that obtains tokens from Azure Active Directory. The user will be able to define their own method of obtaining tokens.
 * @since 1.2.0
 *
 */
public class AzureActiveDirectoryTokenProvider extends TokenProvider
{
    private final BiFunction<String, String, SecurityToken> callback;
    private final String authority;
    
    AzureActiveDirectoryTokenProvider(BiFunction<String, String, SecurityToken> callback, String authority)
    {
    	this.callback = callback;
    	this.authority = (StringUtil.isNullOrEmpty(authority)) ? "https://login.microsoftonline.com/common" : authority;
    }
    
    @Override
    public CompletableFuture<SecurityToken> getSecurityTokenAsync(String audience) {
        return CompletableFuture.supplyAsync(() -> this.callback.apply(audience, this.authority));
    }
}
