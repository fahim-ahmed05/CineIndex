package com.cineindex.companion.data.db

import androidx.room.Database
import androidx.room.RoomDatabase

/**
 * App-owned writable database for history and downloads.
 * Separate from the read-only synced media_index.db.
 */
@Database(
    entities = [HistoryEntity::class, DownloadEntity::class],
    version = 1,
    exportSchema = false
)
abstract class AppDatabase : RoomDatabase() {
    abstract fun historyDao(): HistoryDao
    abstract fun downloadDao(): DownloadDao
}
