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

import java.sql.SQLException;

public class TableDoesntExistException extends Exception {
    public TableDoesntExistException(SQLException e) {
        super(e);
    }
}