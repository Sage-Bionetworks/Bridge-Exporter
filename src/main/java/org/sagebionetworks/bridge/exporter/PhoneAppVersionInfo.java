package org.sagebionetworks.bridge.exporter;

import java.io.IOException;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Strings;

import org.sagebionetworks.bridge.util.BridgeExporterUtil;

public class PhoneAppVersionInfo {
    private final String appVersion;
    private final String phoneInfo;

    public static PhoneAppVersionInfo fromRecord(Item record) {
        String recordId = record.getString("id");

        String appVersion = null;
        String phoneInfo = null;
        String metadataString = record.getString("metadata");
        if (!Strings.isNullOrEmpty(metadataString)) {
            try {
                JsonNode metadataJson = BridgeExporterUtil.JSON_MAPPER.readTree(metadataString);
                appVersion = BridgeExporterUtil.getJsonStringRemoveTabsAndTrim(metadataJson, "appVersion", 48,
                        recordId);
                phoneInfo = BridgeExporterUtil.getJsonStringRemoveTabsAndTrim(metadataJson, "phoneInfo", 48, recordId);
            } catch (IOException ex) {
                // we can recover from this
                System.out.println("[ERROR] Error parsing metadata for record ID " + recordId + ": "
                        + ex.getMessage());
            }
        }

        return new PhoneAppVersionInfo(appVersion, phoneInfo);
    }

    public PhoneAppVersionInfo(String appVersion, String phoneInfo) {
        this.appVersion = appVersion;
        this.phoneInfo = phoneInfo;
    }

    public String getAppVersion() {
        return appVersion;
    }

    public String getPhoneInfo() {
        return phoneInfo;
    }
}
