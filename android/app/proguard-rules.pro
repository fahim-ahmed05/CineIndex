# Add project specific ProGuard rules here.
-keepattributes *Annotation*

# Room
-keep class * extends androidx.room.RoomDatabase
-keep @androidx.room.Entity class *

# Gson
-keepattributes Signature
-keep class com.cineindex.companion.data.config.** { *; }

# Media3
-keep class androidx.media3.** { *; }
