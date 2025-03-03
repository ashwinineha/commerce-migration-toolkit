/*
 * [y] hybris Platform
 *
 * Copyright (c) 2000-2020 SAP SE
 * All rights reserved.
 *
 * This software is the confidential and proprietary information of SAP
 * Hybris ("Confidential Information"). You shall not disclose such
 * Confidential Information and shall use it only in accordance with the
 * terms of the license agreement you entered into with SAP Hybris.
 */
package org.sap.commercemigration.service;

import org.sap.commercemigration.MigrationProgress;
import org.sap.commercemigration.MigrationStatus;
import org.sap.commercemigration.context.CopyContext;

import java.time.OffsetDateTime;
import java.util.Set;

/**
 * Repository to manage Migration Status and Tasks
 */
public interface DatabaseCopyTaskRepository {

    /**
     * Creates a new DB Migration status record
     *
     * @param context
     * @throws Exception
     */
    void createMigrationStatus(CopyContext context) throws Exception;

    /**
     * Updates the Migration status record
     *
     * @param context
     * @param progress
     * @throws Exception
     */
    void setMigrationStatus(CopyContext context, MigrationProgress progress) throws Exception;


    /**
     * Updates the Migration status record from one status to another
     *
     * @param context
     * @param from
     * @param to
     * @throws Exception
     */
    void setMigrationStatus(CopyContext context, MigrationProgress from, MigrationProgress to) throws Exception;

    /**
     * Retrieves the current migration status
     *
     * @param context
     * @return
     * @throws Exception
     */
    MigrationStatus getMigrationStatus(CopyContext context) throws Exception;

    /**
     * Schedules a copy Task
     *
     * @param context        the migration context
     * @param copyItem       the item to copy
     * @param sourceRowCount
     * @param targetNode     the nodeId to perform the copy
     * @throws Exception
     */
    void scheduleTask(CopyContext context, CopyContext.DataCopyItem copyItem, long sourceRowCount, int targetNode) throws Exception;

    /**
     * Retrieves all pending tasks
     *
     * @param context
     * @return
     * @throws Exception
     */
    Set<DatabaseCopyTask> findPendingTasks(CopyContext context) throws Exception;

    /**
     * Updates progress on a Task
     *
     * @param context
     * @param copyItem
     * @param itemCount
     * @throws Exception
     */
    void updateTaskProgress(CopyContext context, CopyContext.DataCopyItem copyItem, long itemCount) throws Exception;

    /**
     * Marks the Task as Completed
     *
     * @param context
     * @param copyItem
     * @param duration
     * @throws Exception
     */
    void markTaskCompleted(CopyContext context, CopyContext.DataCopyItem copyItem, String duration) throws Exception;

    /**
     * Marks the Task as Failed
     *
     * @param context
     * @param copyItem
     * @param error
     * @throws Exception
     */
    void markTaskFailed(CopyContext context, CopyContext.DataCopyItem copyItem, Exception error) throws Exception;

    /**
     * Gets all updated Tasks
     *
     * @param context
     * @param since   offset
     * @return
     * @throws Exception
     */
    Set<DatabaseCopyTask> getUpdatedTasks(CopyContext context, OffsetDateTime since) throws Exception;

    Set<DatabaseCopyTask> getAllTasks(CopyContext context) throws Exception;
}
