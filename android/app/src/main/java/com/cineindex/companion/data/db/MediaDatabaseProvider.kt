package com.cineindex.companion.data.db

import android.content.Context
import androidx.room.Room
import androidx.room.RoomDatabase
import androidx.sqlite.db.SupportSQLiteDatabase
import java.io.File

/**
 * Provides read-only access to the synced media_index.db.
 *
 * This is NOT a Room-managed database — it's an externally built SQLite DB
 * from the CineIndex Python TUI. We open it read-only to avoid any writes
 * or migration issues.
 */
class MediaDatabaseProvider {

    private var database: MediaDatabase? = null
    
    private val _isLoading = kotlinx.coroutines.flow.MutableStateFlow(false)
    val isLoading: kotlinx.coroutines.flow.StateFlow<Boolean> = _isLoading

    fun setLoading(loading: Boolean) {
        _isLoading.value = loading
    }

    /**
     * Open the synced media_index.db at the given path.
     * Returns null if the file doesn't exist.
     */
    fun open(context: Context, dbFile: File): MediaDatabase? {
        close()
        if (!dbFile.exists()) return null

        // Delete existing Room database to force createFromFile to copy the new dbFile
        context.getDatabasePath("media_readonly").delete()
        context.getDatabasePath("media_readonly-shm").delete()
        context.getDatabasePath("media_readonly-wal").delete()

        val db = Room.databaseBuilder(
            context.applicationContext,
            MediaDatabase::class.java,
            "media_readonly"
        )
            .createFromFile(dbFile)
            .fallbackToDestructiveMigration()
            .build()

        database = db
        return db
    }

    fun getDatabase(): MediaDatabase? = database

    fun close() {
        database?.close()
        database = null
    }
}

@androidx.room.Database(
    entities = [MediaEntity::class],
    version = 1,
    exportSchema = false
)
abstract class MediaDatabase : RoomDatabase() {
    abstract fun mediaDao(): MediaDao
}
