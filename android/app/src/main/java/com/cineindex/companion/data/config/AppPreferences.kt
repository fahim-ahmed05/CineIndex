package com.cineindex.companion.data.config

import android.content.Context
import androidx.datastore.core.DataStore
import androidx.datastore.preferences.core.Preferences
import androidx.datastore.preferences.core.edit
import androidx.datastore.preferences.core.stringPreferencesKey
import androidx.datastore.preferences.preferencesDataStore
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.map

private val Context.dataStore: DataStore<Preferences> by preferencesDataStore(name = "cineindex_prefs")

/**
 * App preferences stored via DataStore.
 * Manages folder paths for the synced database and downloads.
 */
class AppPreferences(private val context: Context) {

    companion object {
        private val KEY_DB_FOLDER_URI = stringPreferencesKey("db_folder_uri")
        private val KEY_DOWNLOAD_FOLDER_URI = stringPreferencesKey("download_folder_uri")
    }

    val dbFolderUri: Flow<String?> = context.dataStore.data.map { prefs ->
        prefs[KEY_DB_FOLDER_URI]
    }

    suspend fun setDbFolderUri(uri: String) {
        context.dataStore.edit { prefs ->
            prefs[KEY_DB_FOLDER_URI] = uri
        }
    }
}
