/*
 * [y] hybris Platform
 *
 * Copyright (c) 2000-2016 hybris AG
 * All rights reserved.
 *
 * This software is the confidential and proprietary information of hybris
 * ("Confidential Information"). You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms of the
 * license agreement you entered into with hybris.
 */
package org.sap.move.commercemigrationtest.ccv2;

import java.util.Date;
import java.util.Optional;

public class TypesystemRecord {
    private String typesystemName;
    private String state;
    private Date createdDate;
    private Date modifiedDate;
    private String comment;

    public TypesystemRecord(final String typesystemName, final String state, final Date createdDate, final Date modifiedDate, final String comment) {
        this.typesystemName = typesystemName;
        this.state = state;
        this.createdDate = createdDate;
        this.modifiedDate = modifiedDate;
        this.comment = comment;
    }

    public String getTypesystemName() {
        return typesystemName;
    }

    public String getState() {
        return state;
    }

    public Date getCreatedDate() {
        return createdDate;
    }

    public Date getModifiedDate() {
        return modifiedDate;
    }

    public String getComment() {
        return comment;
    }

    public TypesystemRecord withState(final String state) {
        final TypesystemRecord newRecord = this.copy();
        newRecord.state = state;
        return newRecord;
    }

    public TypesystemRecord withComment(final String comment) {
        final TypesystemRecord newRecord = this.copy();
        newRecord.comment = comment;
        return newRecord;
    }

    /**
     * Clone-like copy utility.
     */
    public TypesystemRecord copy() {

        final Date createdDateCopy = Optional.ofNullable(this.createdDate)
                .map(Date::getTime)
                .map(Date::new)
                .orElse(null);

        final Date modifiedDateCopy = Optional.ofNullable(this.modifiedDate)
                .map(Date::getTime)
                .map(Date::new)
                .orElse(null);

        return new TypesystemRecord(
                this.typesystemName,
                this.state,
                createdDateCopy,
                modifiedDateCopy,
                this.comment);
    }

    @Override
    public String toString() {
        return "TypesystemRecord{" +
                "typesystemName='" + typesystemName + '\'' +
                ", state='" + state + '\'' +
                ", createdDate=" + createdDate +
                ", modifiedDate=" + modifiedDate +
                ", comment='" + comment + '\'' +
                '}';
    }
}