package com.microsoft.azure.servicebus.management;

import java.time.Duration;

class ManagementClientConstants {
    public static Duration MAX_DURATION = Duration.parse("P10675199DT2H48M5.4775807S");

    public static int QUEUE_NAME_MAX_LENGTH = 260;
    public static int TOPIC_NAME_MAX_LENGTH = 260;
    public static int SUBSCRIPTION_NAME_MAX_LENGTH = 50;
    public static int RULE_NAME_MAX_LENGTH = 50;

    public static String ATOM_NS = "http://www.w3.org/2005/Atom";
    public static String SB_NS = "http://schemas.microsoft.com/netservices/2010/10/servicebus/connect";
    public static String XML_SCHEMA_INSTANCE_NS = "http://www.w3.org/2001/XMLSchema-instance";
    public static String XML_SCHEMA_NS = "http://www.w3.org/2001/XMLSchema";
    public static String ATOM_CONTENT_TYPE = "application/atom+xml";
    public static String API_VERSION = "2017-04";
    public static String API_VERSION_QUERY = "api-version=" + API_VERSION;

    public static String ServiceBusSupplementartyAuthorizationHeaderName = "ServiceBusSupplementaryAuthorization";
    public static String ServiceBusDlqSupplementaryAuthorizationHeaderName = "ServiceBusDlqSupplementaryAuthorization";
    public static String HttpErrorSubCodeFormatString = "SubCode=%s";
    public static String ConflictOperationInProgressSubCode =
            String.format(HttpErrorSubCodeFormatString, ExceptionErrorCodes.ConflictOperationInProgress);
    public static String ForbiddenInvalidOperationSubCode =
            String.format(HttpErrorSubCodeFormatString, ExceptionErrorCodes.ForbiddenInvalidOperation);

    public static Duration MIN_ALLOWED_TTL = Duration.ofSeconds(1);
    public static Duration MAX_ALLOWED_TTL = MAX_DURATION;
    public static Duration MIN_LOCK_DURATION = Duration.ofSeconds(5);
    public static Duration MAX_LOCK_DURATION = Duration.ofMinutes(5);
    public static Duration MIN_ALLOWED_AUTODELETE_DURATION = Duration.ofMinutes(5);
    public static Duration MAX_DUPLICATE_HISTORY_DURATION = Duration.ofDays(1);
    public static Duration MIN_DUPLICATE_HISTORY_DURATION = Duration.ofSeconds(20);
    public static int MIN_ALLOWED_MAX_DELIVERYCOUNT = 1;
    public static int MAX_USERMETADATA_LENGTH = 1024;

    public static char[] InvalidEntityPathCharacters = { '@', '?', '#', '*' };

    // Authorization constants
    public static int SupportedClaimsCount = 3;

    public static class ExceptionErrorCodes {
        public static String ConflictOperationInProgress = "40901";
        public static String ForbiddenInvalidOperation = "40301";
    }
}
