package org.sagebionetworks.bridge.exporter;

import com.fasterxml.jackson.annotation.JsonProperty;

public class UploadSchemaKey {
    private final String studyId;
    private final String schemaId;
    private final int rev;

    public UploadSchemaKey(@JsonProperty("studyId") String studyId, @JsonProperty("schemaId") String schemaId,
            @JsonProperty("rev") int rev) {
        this.studyId = studyId;
        this.schemaId = schemaId;
        this.rev = rev;
    }

    public String getStudyId() {
        return studyId;
    }

    public String getSchemaId() {
        return schemaId;
    }

    public int getRev() {
        return rev;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        UploadSchemaKey that = (UploadSchemaKey) o;

        if (rev != that.rev) {
            return false;
        }
        if (schemaId != null ? !schemaId.equals(that.schemaId) : that.schemaId != null) {
            return false;
        }
        if (studyId != null ? !studyId.equals(that.studyId) : that.studyId != null) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = studyId != null ? studyId.hashCode() : 0;
        result = 31 * result + (schemaId != null ? schemaId.hashCode() : 0);
        result = 31 * result + rev;
        return result;
    }

    @Override
    public String toString() {
        return studyId + "-" + schemaId + "-v" + rev;
    }
}
