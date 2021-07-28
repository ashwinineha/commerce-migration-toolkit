/*
 * [y] hybris Platform
 *
 * Copyright (c) 2019 SAP SE or an SAP affiliate company. All rights reserved.
 *
 * This software is the confidential and proprietary information of SAP
 * ("Confidential Information"). You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms of the
 * license agreement you entered into with SAP.
 */
package org.sap.move.commercemigrationtest.ccv2;

import org.slf4j.LoggerFactory;

import java.util.function.Supplier;

/**
 * Simple retry mechanism for retrying a command based on exception.
 */
public class RetryCommand<T> {
    private final int maxRetries;
    private final int secondsBetweenRetries;

    public RetryCommand(final int maxRetries, final int secondsBetweenRetries) {
        this.maxRetries = maxRetries;
        this.secondsBetweenRetries = secondsBetweenRetries;
    }

    // Execute the function and if fails then retry till range is exhausted.
    public T run(final Supplier<T> function) {
        try {
            return function.get();
        } catch (final Exception e) {
            LoggerFactory.getLogger(this.getClass()).error("Exception thrown during execution of command.", e);
            return this.retry(function);
        }
    }

    private T retry(final Supplier<T> function) {
        LoggerFactory.getLogger(this.getClass()).error(
                "[FAILED] - Command failed, will be retried {} times with delay of {} seconds.", this.maxRetries,
                this.secondsBetweenRetries);
        int retryCounter = 0;
        while (retryCounter < this.maxRetries) {
            try {
                return function.get();
            } catch (final Exception ex) {
                retryCounter++;
                LoggerFactory.getLogger(this.getClass()).error(
                        "[FAILED] - Command failed on retry {} of {}", +retryCounter, this.maxRetries, ex);
                if (retryCounter >= this.maxRetries) {
                    LoggerFactory.getLogger(this.getClass()).error("Max retries exceeded.");
                    break;
                }
            }
            this.waitTillNextRetry();
        }
        throw new RetryFailedException("[FAILED] - Maximum number of retries [" + this.maxRetries + "] exhausted.");
    }

    private void waitTillNextRetry() {
        try {
            Thread.sleep(this.secondsBetweenRetries * 1000L);
        } catch (final InterruptedException e) {
            LoggerFactory.getLogger(this.getClass()).error("Interruption Expcetion", e);
            Thread.currentThread().interrupt();
        }
    }
}