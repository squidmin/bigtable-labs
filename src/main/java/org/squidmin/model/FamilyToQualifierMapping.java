package org.squidmin.model;

import lombok.Data;

import java.util.Set;

@Data
public class FamilyToQualifierMapping {

    private String columnFamilyName;
    private Set<String> rowKeyFields;
    private Set<String> qualifierNames;

}
