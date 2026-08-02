package com.cineindex.companion.di

import android.content.Context
import androidx.room.Room
import com.cineindex.companion.data.config.AppPreferences
import com.cineindex.companion.data.config.RootsConfig
import com.cineindex.companion.data.db.AppDatabase
import com.cineindex.companion.data.db.HistoryDao
import com.cineindex.companion.data.db.MediaDatabaseProvider
import dagger.Module
import dagger.Provides
import dagger.hilt.InstallIn
import dagger.hilt.android.qualifiers.ApplicationContext
import dagger.hilt.components.SingletonComponent
import javax.inject.Singleton

@Module
@InstallIn(SingletonComponent::class)
object AppModule {

    @Provides
    @Singleton
    fun provideAppDatabase(@ApplicationContext context: Context): AppDatabase {
        return Room.databaseBuilder(
            context.applicationContext,
            AppDatabase::class.java,
            "cineindex_app.db"
        ).build()
    }

    @Provides
    fun provideHistoryDao(db: AppDatabase): HistoryDao = db.historyDao()

    @Provides
    @Singleton
    fun provideMediaDatabaseProvider(): MediaDatabaseProvider = MediaDatabaseProvider()

    @Provides
    @Singleton
    fun provideAppPreferences(@ApplicationContext context: Context): AppPreferences {
        return AppPreferences(context)
    }

    @Provides
    @Singleton
    fun provideRootsConfig(): RootsConfig = RootsConfig()
}
