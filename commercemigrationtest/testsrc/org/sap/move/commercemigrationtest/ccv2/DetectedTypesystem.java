/*
 * [y] hybris Platform
 *
 * Copyright (c) 2018 SAP SE or an SAP affiliate company. All rights reserved.
 *
 * This software is the confidential and proprietary information of SAP
 * ("Confidential Information"). You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms of the
 * license agreement you entered into with SAP.
 */
package org.sap.move.commercemigrationtest.ccv2;

import java.util.Date;

public class DetectedTypesystem {
    private String typesystemName;
    private String attributeTableName;
    private Date updated;

    public DetectedTypesystem(final String typesystemName, final String attributeTableName) {
        this.typesystemName = typesystemName;
        this.attributeTableName = attributeTableName;
    }

    public String getTypesystemName() {
        return typesystemName;
    }

    public String getAttributeTableName() {
        return attributeTableName;
    }

    public Date getUpdated() {
        return updated;
    }

    public void setUpdated(final Date updated) {
        this.updated = updated;
    }
}