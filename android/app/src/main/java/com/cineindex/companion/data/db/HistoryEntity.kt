package com.cineindex.companion.data.db

import androidx.room.Entity
import androidx.room.PrimaryKey

/**
 * Tracks watch history with resume position.
 * Stored in the app's own writable database (not the synced read-only DB).
 */
@Entity(tableName = "history")
data class HistoryEntity(
    @PrimaryKey val url: String,
    val filename: String,
    val path: String,
    val root: String,
    val playedAt: Long,
    val positionMs: Long = 0,
    val durationMs: Long = 0
)
