/*
 * [y] hybris Platform
 *
 * Copyright (c) 2000-2016 SAP SE or an SAP affiliate company.
 * All rights reserved.
 *
 * This software is the confidential and proprietary information of SAP
 * ("Confidential Information"). You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms of the
 * license agreement you entered into with SAP.
 */
package org.sap.move.commercemigrationtest.ccv2;

/**
 * Retry failed exception.
 */
public class RetryFailedException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public RetryFailedException() {
    }

    public RetryFailedException(final String message) {
        super(message);
    }

    public RetryFailedException(final String message, final Throwable cause) {
        super(message, cause);
    }

    public RetryFailedException(final Throwable cause) {
        super(cause);
    }
}