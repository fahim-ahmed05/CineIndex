package com.cineindex.companion.data.db

import androidx.room.Entity
import androidx.room.Index
import androidx.room.PrimaryKey

/**
 * Read-only entity matching the 'media' table in the synced media_index.db.
 * Schema must match exactly what the Python TUI creates.
 */
@Entity(
    tableName = "media",
    indices = [
        Index("root"),
        Index("path"),
        Index("filename")
    ]
)
data class MediaEntity(
    @PrimaryKey val url: String,
    val root: String,
    val path: String,
    val filename: String,
    val modified: String?,
    val size: String?
)
