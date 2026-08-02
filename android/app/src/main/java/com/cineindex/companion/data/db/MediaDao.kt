package com.cineindex.companion.data.db

import androidx.room.Dao
import androidx.room.Query

/**
 * Read-only DAO for the synced media_index.db.
 * Uses FTS5 for fast full-text search and standard queries for playlist building.
 */
@Dao
interface MediaDao {

    /**
     * FTS5-powered search. Matches against filename and path columns.
     * Caller should format query as FTS5 match syntax (e.g., "breaking* bad*").
     */
    @Query("""
        SELECT media.* FROM media
        JOIN media_fts ON media_fts.rowid = media.rowid
        WHERE media_fts MATCH :query
        LIMIT :limit
    """)
    suspend fun searchFts(query: String, limit: Int = 100): List<MediaEntity>

    /**
     * Fallback substring search when FTS5 query syntax fails.
     */
    @Query("SELECT * FROM media WHERE filename LIKE '%' || :query || '%' ORDER BY filename LIMIT :limit")
    suspend fun searchLike(query: String, limit: Int = 100): List<MediaEntity>

    /**
     * Get all media entries in a specific directory path (for same-dir playlist fallback).
     */
    @Query("SELECT * FROM media WHERE path = :path ORDER BY filename")
    suspend fun getByPath(path: String): List<MediaEntity>

    /**
     * Get a single media entry by URL.
     */
    @Query("SELECT * FROM media WHERE url = :url")
    suspend fun getByUrl(url: String): MediaEntity?

    /**
     * Get all media entries for roots sharing the same set of root URLs (cross-root playlist).
     */
    @Query("SELECT * FROM media WHERE root IN (:roots)")
    suspend fun getByRoots(roots: List<String>): List<MediaEntity>

    /**
     * Get all media entries (for broad library show-name search).
     * Use with caution on large databases — prefer targeted queries.
     */
    @Query("SELECT url, root, path, filename, modified, size FROM media")
    suspend fun getAll(): List<MediaEntity>

    /**
     * Get distinct root URLs present in the database.
     */
    @Query("SELECT DISTINCT root FROM media")
    suspend fun getRoots(): List<String>

    /**
     * Get total count of indexed media files.
     */
    @Query("SELECT COUNT(*) FROM media")
    suspend fun getCount(): Int
}
