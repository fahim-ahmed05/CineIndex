package com.cineindex.companion.data.config

import com.google.gson.Gson
import com.google.gson.reflect.TypeToken
import java.io.File

/**
 * Reads roots.json to extract per-root display settings.
 * Matches the Python TUI's roots.json format (grouped or legacy flat).
 */
class RootsConfig {

    data class RootSettings(
        val tag: String,
        val dotsToSpaces: Boolean
    )

    private var rootMap: Map<String, RootSettings> = emptyMap()

    /**
     * Load roots.json from the given file.
     * Supports both grouped format (with "roots" array) and legacy flat format.
     */
    fun load(file: File) {
        if (!file.exists()) {
            rootMap = emptyMap()
            return
        }

        try {
            val json = file.readText(Charsets.UTF_8)
            val gson = Gson()
            val listType = object : TypeToken<List<Map<String, Any>>>() {}.type
            val raw: List<Map<String, Any>> = gson.fromJson(json, listType)

            val map = mutableMapOf<String, RootSettings>()

            for (entry in raw) {
                val roots = entry["roots"]
                if (roots is List<*>) {
                    // Grouped format: { "tag": "...", "roots": [{"url": "..."}, ...], ... }
                    val tag = (entry["tag"] as? String)?.trim() ?: ""
                    val globalDotsToSpaces = entry["dots_to_spaces"] as? Boolean ?: false

                    for (rootItem in roots) {
                        if (rootItem is Map<*, *>) {
                            val url = normalizeUrl((rootItem["url"] as? String)?.trim() ?: "")
                            if (url.isEmpty()) continue
                            val dotsToSpaces = rootItem["dots_to_spaces"] as? Boolean ?: globalDotsToSpaces
                            map[url] = RootSettings(tag = tag, dotsToSpaces = dotsToSpaces)
                        }
                    }
                } else {
                    // Legacy flat format: { "url": "...", "tag": "...", ... }
                    val url = normalizeUrl((entry["url"] as? String)?.trim() ?: "")
                    if (url.isEmpty()) continue
                    val tag = (entry["tag"] as? String)?.trim() ?: ""
                    val dotsToSpaces = entry["dots_to_spaces"] as? Boolean ?: false
                    map[url] = RootSettings(tag = tag, dotsToSpaces = dotsToSpaces)
                }
            }

            rootMap = map
        } catch (e: Exception) {
            rootMap = emptyMap()
        }
    }

    fun getSettings(rootUrl: String): RootSettings? = rootMap[normalizeUrl(rootUrl)]

    fun getTag(rootUrl: String): String = getSettings(rootUrl)?.tag ?: ""

    fun dotsToSpaces(rootUrl: String): Boolean = getSettings(rootUrl)?.dotsToSpaces ?: false

    private fun normalizeUrl(url: String): String {
        return if (url.isNotEmpty() && !url.endsWith("/")) "$url/" else url
    }
}
