package com.cineindex.companion.data.db

import androidx.room.Entity
import androidx.room.PrimaryKey

/**
 * Tracks download state and progress.
 * Stored in the app's own writable database.
 */
@Entity(tableName = "downloads")
data class DownloadEntity(
    @PrimaryKey val url: String,
    val filename: String,
    val localPath: String,
    val totalBytes: Long = -1,
    val downloadedBytes: Long = 0,
    val status: Int = STATUS_PENDING,
    val startedAt: Long = System.currentTimeMillis()
) {
    companion object {
        const val STATUS_PENDING = 0
        const val STATUS_DOWNLOADING = 1
        const val STATUS_PAUSED = 2
        const val STATUS_COMPLETED = 3
        const val STATUS_FAILED = 4
    }
}
