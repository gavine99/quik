/*
 * Copyright (C) 2017 Moez Bhatti <moez.bhatti@gmail.com>
 *
 * This file is part of QKSMS.
 *
 * QKSMS is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * QKSMS is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with QKSMS.  If not, see <http://www.gnu.org/licenses/>.
 */
package dev.octoshrimpy.quik.manager

import android.content.Context
import android.util.Log
import com.squareup.moshi.Json
import com.squareup.moshi.JsonClass
import com.squareup.moshi.Moshi
import com.squareup.moshi.Types
import dev.octoshrimpy.quik.common.util.extensions.versionCode
import dev.octoshrimpy.quik.util.Preferences
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import javax.inject.Inject

class ChangelogManagerImpl @Inject constructor(
    private val context: Context,
    private val moshi: Moshi,
    private val prefs: Preferences
) : ChangelogManager {

    override fun didUpdate(): Boolean = (prefs.changelogVersion.get() ?: 0) != context.versionCode

    override suspend fun getChangelog(): ChangelogManager.CumulativeChangelog {
        val listType = Types.newParameterizedType(List::class.java, Changeset::class.java)
        val adapter = moshi.adapter<List<Changeset>>(listType)

        return withContext(Dispatchers.IO) {
            try {
                val changelogs = context.assets.open("changelog.json").bufferedReader().use { it.readText() }
                        .let(adapter::fromJson)
                        .orEmpty()
                        .sortedBy { changelog -> changelog.versionCode }
                        .filter { changelog ->
                            changelog.versionCode in (prefs.changelogVersion.get() ?: 0).inc()..context.versionCode
                        }

                ChangelogManager.CumulativeChangelog(
                        added = changelogs.fold(listOf()) { acc, changelog -> acc + changelog.added.orEmpty()},
                        improved = changelogs.fold(listOf()) { acc, changelog -> acc + changelog.improved.orEmpty()},
                        removed = changelogs.fold(listOf()) { acc, changelog -> acc + changelog.removed.orEmpty()},
                        fixed = changelogs.fold(listOf()) { acc, changelog -> acc + changelog.fixed.orEmpty()}
                )

            } catch (e: Exception) {
                Log.e("ChangelogManager", "Error parsing changelog JSON", e)
                ChangelogManager.CumulativeChangelog(emptyList(), emptyList(), emptyList(), emptyList())
            }
        }
    }

    override fun markChangelogSeen() {
        prefs.changelogVersion.set(context.versionCode)
    }

    @JsonClass(generateAdapter = false)
    data class Changeset(
        @Json(name = "added") val added: List<String>?,
        @Json(name = "improved") val improved: List<String>?,
        @Json(name = "removed") val removed: List<String>?,
        @Json(name = "fixed") val fixed: List<String>?,
        @Json(name = "versionName") val versionName: String,
        @Json(name = "versionCode") val versionCode: Int
    )

}
