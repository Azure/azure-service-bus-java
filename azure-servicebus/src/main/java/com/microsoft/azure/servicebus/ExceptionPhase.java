package com.microsoft.azure.servicebus;

public enum ExceptionPhase {
	RECEIVE,
	RENEWLOCK,
	COMPLETE,
	ABANDON,
	USERCALLBACK
}
