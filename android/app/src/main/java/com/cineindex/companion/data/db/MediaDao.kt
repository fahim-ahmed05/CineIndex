package com.cineindex.companion.data.db

import android.database.sqlite.SQLiteDatabase
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext

/**
 * Read-only DAO for the synced media_index.db.
 * Uses pure SQLite to avoid strict Room schema verification issues.
 */
class MediaDao(private val db: SQLiteDatabase) {

    private fun mapCursor(cursor: android.database.Cursor): MediaEntity {
        return MediaEntity(
            url = cursor.getString(cursor.getColumnIndexOrThrow("url")),
            root = cursor.getString(cursor.getColumnIndexOrThrow("root")),
            path = cursor.getString(cursor.getColumnIndexOrThrow("path")),
            filename = cursor.getString(cursor.getColumnIndexOrThrow("filename")),
            modified = cursor.getString(cursor.getColumnIndexOrThrow("modified")),
            size = cursor.getString(cursor.getColumnIndexOrThrow("size"))
        )
    }

    suspend fun searchFts(query: String, limit: Int = 100): List<MediaEntity> = withContext(Dispatchers.IO) {
        val list = mutableListOf<MediaEntity>()
        db.rawQuery(
            "SELECT media.* FROM media JOIN media_fts ON media_fts.rowid = media.rowid WHERE media_fts MATCH ? ORDER BY rank, media.filename LIMIT ?",
            arrayOf(query, limit.toString())
        ).use { cursor ->
            while (cursor.moveToNext()) list.add(mapCursor(cursor))
        }
        list
    }

    suspend fun searchLike(query: String, limit: Int = 100): List<MediaEntity> = withContext(Dispatchers.IO) {
        val list = mutableListOf<MediaEntity>()
        db.rawQuery(
            "SELECT * FROM media WHERE filename LIKE '%' || ? || '%' ORDER BY LENGTH(filename), filename LIMIT ?",
            arrayOf(query, limit.toString())
        ).use { cursor ->
            while (cursor.moveToNext()) list.add(mapCursor(cursor))
        }
        list
    }

    suspend fun getByPath(path: String): List<MediaEntity> = withContext(Dispatchers.IO) {
        val list = mutableListOf<MediaEntity>()
        db.rawQuery("SELECT * FROM media WHERE path = ? ORDER BY filename", arrayOf(path)).use { cursor ->
            while (cursor.moveToNext()) list.add(mapCursor(cursor))
        }
        list
    }

    suspend fun getByUrl(url: String): MediaEntity? = withContext(Dispatchers.IO) {
        db.rawQuery("SELECT * FROM media WHERE url = ?", arrayOf(url)).use { cursor ->
            if (cursor.moveToFirst()) mapCursor(cursor) else null
        }
    }

    suspend fun getByRoots(roots: List<String>): List<MediaEntity> = withContext(Dispatchers.IO) {
        if (roots.isEmpty()) return@withContext emptyList()
        val placeholders = roots.joinToString(",") { "?" }
        val list = mutableListOf<MediaEntity>()
        db.rawQuery("SELECT * FROM media WHERE root IN ($placeholders)", roots.toTypedArray()).use { cursor ->
            while (cursor.moveToNext()) list.add(mapCursor(cursor))
        }
        list
    }

    suspend fun getAll(): List<MediaEntity> = withContext(Dispatchers.IO) {
        val list = mutableListOf<MediaEntity>()
        db.rawQuery("SELECT url, root, path, filename, modified, size FROM media", null).use { cursor ->
            while (cursor.moveToNext()) list.add(mapCursor(cursor))
        }
        list
    }

    suspend fun getRoots(): List<String> = withContext(Dispatchers.IO) {
        val list = mutableListOf<String>()
        db.rawQuery("SELECT DISTINCT root FROM media", null).use { cursor ->
            while (cursor.moveToNext()) list.add(cursor.getString(0))
        }
        list
    }

    suspend fun getCount(): Int = withContext(Dispatchers.IO) {
        db.rawQuery("SELECT COUNT(*) FROM media", null).use { cursor ->
            if (cursor.moveToFirst()) cursor.getInt(0) else 0
        }
    }
}
