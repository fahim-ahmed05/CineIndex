package com.cineindex.companion.data.db

import android.content.Context
import android.database.sqlite.SQLiteDatabase
import java.io.File

/**
 * Provides read-only access to the synced media_index.db.
 *
 * This is NOT a Room-managed database — it's an externally built SQLite DB
 * from the CineIndex Python TUI. We open it read-only to avoid any writes
 * or migration issues.
 */
class MediaDatabaseProvider {
    private var database: SQLiteDatabase? = null
    private var dao: MediaDao? = null
    
    private val _isLoading = kotlinx.coroutines.flow.MutableStateFlow(false)
    val isLoading: kotlinx.coroutines.flow.StateFlow<Boolean> = _isLoading

    fun setLoading(loading: Boolean) {
        _isLoading.value = loading
    }

    fun open(context: Context, dbFile: File): MediaDao? {
        close()
        if (!dbFile.exists()) return null

        return try {
            val db = SQLiteDatabase.openDatabase(
                dbFile.absolutePath, 
                null, 
                SQLiteDatabase.OPEN_READONLY
            )
            database = db
            val mediaDao = MediaDao(db)
            dao = mediaDao
            mediaDao
        } catch (e: Exception) {
            e.printStackTrace()
            null
        }
    }

    fun getDao(): MediaDao? {
        return dao
    }

    fun close() {
        database?.close()
        database = null
        dao = null
    }
}
