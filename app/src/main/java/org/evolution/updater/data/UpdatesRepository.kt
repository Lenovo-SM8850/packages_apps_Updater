/*
 * SPDX-FileCopyrightText: The LineageOS Project
 * SPDX-License-Identifier: Apache-2.0
 */

package org.evolution.updater.data

import android.util.Log
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flowOn
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.withContext
import kotlinx.serialization.SerializationException
import org.evolution.updater.data.source.local.UpdatesLocalDataSource
import org.evolution.updater.data.source.network.UpdatesNetworkDataSource
import org.evolution.updater.data.source.network.toSupportInfo
import org.evolution.updater.data.source.network.toUpdate
import org.evolution.updater.deviceinfo.DeviceInfoUtils
import org.evolution.updater.notifications.NotificationHelper
import org.evolution.updater.util.NetworkMonitor
import org.evolution.updater.util.SystemUpdateInfoPublisher
import java.io.IOException

private const val TAG = "UpdatesRepository"

class UpdatesRepository(
    private val networkMonitor: NetworkMonitor,
    private val notificationHelper: NotificationHelper,
    private val networkDataSource: UpdatesNetworkDataSource,
    private val localDataSource: UpdatesLocalDataSource,
    private val systemUpdateInfoPublisher: SystemUpdateInfoPublisher,
    private val appStateRepository: AppStateRepository,
) {
    private val mutex = Mutex()

    fun observeLocalUpdates(): Flow<List<Update>> = localDataSource.observeUpdates()
        .map { updates -> updates.filter { isShown(it) } }
        .flowOn(Dispatchers.IO)

    suspend fun removeObsoleteUpdates() = mutex.withLock {
        removeObsoleteUpdatesLocked()
        systemUpdateInfoPublisher.publish()
    }

    private suspend fun removeObsoleteUpdatesLocked(): List<Update> = withContext(Dispatchers.IO) {
        val updates = localDataSource.getUpdates()
        val (available, obsolete) = updates.partition { isShown(it) }
        obsolete.forEach {
            if (it.downloadId != Update.LOCAL_ID) it.file?.delete()
            localDataSource.removeUpdate(it.downloadId)
        }
        if (available.none { it.downloadId != Update.LOCAL_ID }) {
            notificationHelper.cancelNewUpdatesNotification()
        }
        available
    }

    /**
     * Fetches available updates from the server, syncs the local database, and posts a
     * notification if new updates are found. Callers observe [observeLocalUpdates] for results —
     * Room only emits when the stored data actually changes.
     *
     * @return the timestamp of the fetch, or null if skipped due to no network.
     * @throws IOException on network or HTTP errors.
     * @throws SerializationException if the response cannot be parsed.
     */
    suspend fun fetchUpdates(): Long? = mutex.withLock {
        val localUpdates = removeObsoleteUpdatesLocked().associateBy { it.downloadId }
        if (!networkMonitor.currentNetworkState.isOnline) {
            systemUpdateInfoPublisher.publish()
            return@withLock null
        }

        val networkResponse = withContext(Dispatchers.IO) {
            networkDataSource.fetchUpdates()
        }
        appStateRepository.setSupportInfo(
            networkResponse.maxByOrNull { it.timestamp }?.toSupportInfo() ?: SupportInfo()
        )
        val networkUpdates = networkResponse.map { it.toUpdate() }.filter { filterUpdates(it) }

        if (networkUpdates.isEmpty()) {
            systemUpdateInfoPublisher.publish()
            return@withLock System.currentTimeMillis()
        }

        val networkIds = networkUpdates.map { it.downloadId }.toSet()

        if (localUpdates.isNotEmpty() && networkUpdates.any { it.downloadId !in localUpdates }) {
            notificationHelper.showNewUpdatesNotification()
        }

        withContext(Dispatchers.IO) {
            // Merge local state into each network update and upsert into the DB.
            // Room's observeUpdates() Flow will emit automatically if anything changed.
            networkUpdates.forEach { networkUpdate ->
                val local = localUpdates[networkUpdate.downloadId]
                val update = if (local != null && local.status.persistentStatus > 0) {
                    networkUpdate.copy(status = local.status, file = local.file)
                } else {
                    networkUpdate
                }
                localDataSource.addUpdate(update)
            }

            // Delete temp files and DB entries for updates no longer advertised by the server.
            localUpdates.values.filter {
                it.downloadId !in networkIds && it.downloadId != Update.LOCAL_ID &&
                        it.downloadUrl != null
            }.forEach {
                it.file?.delete()
                localDataSource.removeUpdate(it.downloadId)
            }
        }
        systemUpdateInfoPublisher.publish()

        System.currentTimeMillis()
    }

    // Imported packages stay listed, with their blocked reason, until they are the running build.
    private fun isShown(update: Update): Boolean =
        if (update.downloadId == Update.LOCAL_ID) {
            update.timestamp != DeviceInfoUtils.buildDateTimestamp
        } else {
            filterUpdates(update)
        }

    private fun filterUpdates(update: Update): Boolean {
        val isCurrentBuild = update.timestamp == DeviceInfoUtils.buildDateTimestamp
        val isOlderBuild = update.timestamp < DeviceInfoUtils.buildDateTimestamp

        if (!DeviceInfoUtils.isDowngradingAllowed && (isOlderBuild || isCurrentBuild)) {
            Log.d(TAG, "${update.name} is not newer than the current build")
            return false
        }

        if (update.osSdkLevel < DeviceInfoUtils.sdkLevel) {
            Log.d(TAG, "${update.name} is older than current Android version")
            return false
        }
        return true
    }
}
