package com.cineindex.companion.data.search

import com.cineindex.companion.data.db.MediaDao
import com.cineindex.companion.data.db.MediaEntity
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext

/**
 * FTS5-powered search engine for the media database.
 * Transforms user input into FTS5 match syntax with prefix matching.
 */
class SearchEngine(private val mediaDao: MediaDao) {

    /**
     * Search media entries using FTS5 full-text search.
     * Falls back to LIKE search if FTS5 query syntax fails.
     *
     * @param query User's search input
     * @param limit Maximum number of results
     * @return List of matching media entries
     */
    suspend fun search(query: String, limit: Int = 100): List<MediaEntity> = withContext(Dispatchers.IO) {
        val trimmed = query.trim()
        if (trimmed.isEmpty()) return@withContext emptyList()

        // Build FTS5 match expression: each word becomes a prefix match
        // "breaking bad" -> "breaking* bad*"
        val ftsQuery = buildFtsQuery(trimmed)

        try {
            val results = mediaDao.searchFts(ftsQuery, limit)
            if (results.isNotEmpty()) return@withContext results

            // FTS5 returned nothing — try LIKE fallback
            mediaDao.searchLike(trimmed, limit)
        } catch (e: Exception) {
            // FTS5 syntax error or table missing — fall back to LIKE
            try {
                mediaDao.searchLike(trimmed, limit)
            } catch (e2: Exception) {
                emptyList()
            }
        }
    }

    /**
     * Build an FTS5 MATCH expression from user input.
     * Each word gets a * suffix for prefix matching.
     * Special characters that break FTS5 syntax are stripped.
     *
     * Examples:
     *   "breaking bad" -> "breaking* bad*"
     *   "S01E03"       -> "S01E03*"
     *   "game.of"      -> "game of*"  (dots become spaces in FTS5 tokenization)
     */
    private fun buildFtsQuery(input: String): String {
        // Replace common separators with spaces for tokenization
        val normalized = input
            .replace('.', ' ')
            .replace('_', ' ')
            .replace('-', ' ')

        // Strip characters that break FTS5 syntax
        val cleaned = normalized.replace(Regex("""[^\w\s]"""), "")

        val words = cleaned.split(Regex("""\s+""")).filter { it.isNotEmpty() }
        if (words.isEmpty()) return "\"\""

        // Each word gets prefix matching
        return words.joinToString(" ") { "$it*" }
    }
}
