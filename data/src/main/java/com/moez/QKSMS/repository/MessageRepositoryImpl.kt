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
package dev.octoshrimpy.quik.repository

import android.app.AlarmManager
import android.app.PendingIntent
import android.content.ContentUris
import android.content.ContentValues
import android.content.Context
import android.content.Intent
import android.net.Uri
import android.os.Build
import android.os.Environment
import android.provider.MediaStore
import android.provider.Telephony
import android.provider.Telephony.Mms
import android.provider.Telephony.Sms
import android.telephony.SmsManager
import android.webkit.MimeTypeMap
import androidx.core.content.contentValuesOf
import com.google.android.mms.ContentType
import com.google.android.mms.MMSPart
import com.google.android.mms.pdu_alt.MultimediaMessagePdu
import com.google.android.mms.pdu_alt.PduPersister
import com.klinker.android.send_message.SmsManagerFactory
import com.klinker.android.send_message.StripAccents
import com.klinker.android.send_message.Transaction
import dev.octoshrimpy.quik.common.util.extensions.now
import dev.octoshrimpy.quik.compat.TelephonyCompat
import dev.octoshrimpy.quik.extensions.anyOf
import dev.octoshrimpy.quik.extensions.getResourceBytes
import dev.octoshrimpy.quik.extensions.isImage
import dev.octoshrimpy.quik.extensions.isVideo
import dev.octoshrimpy.quik.extensions.resourceExists
import dev.octoshrimpy.quik.manager.ActiveConversationManager
import dev.octoshrimpy.quik.manager.KeyManager
import dev.octoshrimpy.quik.model.Attachment
import dev.octoshrimpy.quik.model.Conversation
import dev.octoshrimpy.quik.model.Message
import dev.octoshrimpy.quik.model.MmsPart
import dev.octoshrimpy.quik.receiver.SendSmsReceiver
import dev.octoshrimpy.quik.receiver.SmsDeliveredReceiver
import dev.octoshrimpy.quik.receiver.SmsSentReceiver
import dev.octoshrimpy.quik.util.PhoneNumberUtils
import dev.octoshrimpy.quik.util.Preferences
import dev.octoshrimpy.quik.util.ImageUtils
import dev.octoshrimpy.quik.util.ImageUtils.Companion.UNSPECIFIED
import dev.octoshrimpy.quik.util.tryOrNull
import io.realm.Case
import io.realm.Realm
import io.realm.RealmQuery
import io.realm.RealmResults
import io.realm.Sort
import timber.log.Timber
import java.util.concurrent.TimeUnit
import javax.inject.Inject
import javax.inject.Singleton
import kotlin.collections.ArrayList

@Singleton
open class MessageRepositoryImpl @Inject constructor(
    private val activeConversationManager: ActiveConversationManager,
    private val context: Context,
    private val messageIds: KeyManager,
    private val phoneNumberUtils: PhoneNumberUtils,
    private val prefs: Preferences,
    private val syncRepository: SyncRepository
) : MessageRepository {

    private fun getMessagesBase(threadId: Long, query: String): RealmQuery<Message> {
        return Realm.getDefaultInstance()
            .where(Message::class.java)
            .equalTo("threadId", threadId)
            .let {
                when (query.isEmpty()) {
                    true -> it
                    false -> it
                        .beginGroup()
                        .contains("body", query, Case.INSENSITIVE)
                        .or()
                        .contains("parts.text", query, Case.INSENSITIVE)
                        .endGroup()
                }
            }
            .sort("date")
    }

    override fun getMessages(threadId: Long, query: String): RealmResults<Message> {
        return getMessagesBase(threadId, query).findAllAsync()
    }

    override fun getMessagesSync(threadId: Long, query: String): RealmResults<Message> {
        return getMessagesBase(threadId, query).findAll()
    }

    override fun getMessage(id: Long): Message? {
        return Realm.getDefaultInstance()
                .also { realm -> realm.refresh() }
                .where(Message::class.java)
                .equalTo("id", id)
                .findFirst()
    }

    override fun getMessageForPart(id: Long): Message? {
        return Realm.getDefaultInstance()
                .where(Message::class.java)
                .equalTo("parts.id", id)
                .findFirst()
    }

    override fun getLastIncomingMessage(threadId: Long): RealmResults<Message> {
        return Realm.getDefaultInstance()
                .where(Message::class.java)
                .equalTo("threadId", threadId)
                .beginGroup()
                .beginGroup()
                .equalTo("type", "sms")
                .`in`("boxId", arrayOf(Sms.MESSAGE_TYPE_INBOX, Sms.MESSAGE_TYPE_ALL))
                .endGroup()
                .or()
                .beginGroup()
                .equalTo("type", "mms")
                .`in`("boxId", arrayOf(Mms.MESSAGE_BOX_INBOX, Mms.MESSAGE_BOX_ALL))
                .endGroup()
                .endGroup()
                .sort("date", Sort.DESCENDING)
                .findAll()
    }

    override fun getUnreadCount(): Long {
        return Realm.getDefaultInstance().use { realm ->
            realm.refresh()
            realm.where(Conversation::class.java)
                    .equalTo("archived", false)
                    .equalTo("blocked", false)
                    .equalTo("lastMessage.read", false)
                    .count()
        }
    }

    override fun getPart(id: Long): MmsPart? {
        return Realm.getDefaultInstance()
                .where(MmsPart::class.java)
                .equalTo("id", id)
                .findFirst()
    }

    override fun getPartsForConversation(threadId: Long): RealmResults<MmsPart> {
        return Realm.getDefaultInstance()
                .where(MmsPart::class.java)
                .equalTo("messages.threadId", threadId)
                .beginGroup()
                .contains("type", "image/")
                .or()
                .contains("type", "video/")
                .endGroup()
                .sort("id", Sort.DESCENDING)
                .findAllAsync()
    }

    override fun savePart(id: Long): Uri? {
        val part = getPart(id) ?: return null

        val extension = MimeTypeMap.getSingleton().getExtensionFromMimeType(part.type) ?: return null
        val date = part.messages?.first()?.date
        val fileName = part.name?.takeIf { name -> name.endsWith(extension) }
                ?: "${part.type.split("/").last()}_$date.$extension"

        val values = contentValuesOf(
                MediaStore.MediaColumns.DISPLAY_NAME to fileName,
                MediaStore.MediaColumns.MIME_TYPE to part.type,
        )

        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.Q) {
            values.put(MediaStore.MediaColumns.IS_PENDING, 1)
            values.put(MediaStore.MediaColumns.RELATIVE_PATH, when {
                part.isImage() -> "${Environment.DIRECTORY_PICTURES}/QUIK"
                part.isVideo() -> "${Environment.DIRECTORY_MOVIES}/QUIK"
                else -> "${Environment.DIRECTORY_DOWNLOADS}/QUIK"
            })
        }

        val contentUri = when {
            part.isImage() -> MediaStore.Images.Media.EXTERNAL_CONTENT_URI
            part.isVideo() -> MediaStore.Video.Media.EXTERNAL_CONTENT_URI
            Build.VERSION.SDK_INT >= Build.VERSION_CODES.Q -> MediaStore.Downloads.EXTERNAL_CONTENT_URI
            else -> MediaStore.Files.getContentUri("external")
        }
        val resolver = context.contentResolver
        val uri = resolver.insert(contentUri, values)
        Timber.v("Saving $fileName (${part.type}) to $uri")

        uri?.let {
            resolver.openOutputStream(uri)?.use { outputStream ->
                context.contentResolver.openInputStream(part.getUri())?.use { inputStream ->
                    inputStream.copyTo(outputStream, 1024)
                }
            }
            Timber.v("Saved $fileName (${part.type}) to $uri")

            if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.Q) {
                resolver.update(
                    uri,
                    contentValuesOf(MediaStore.MediaColumns.IS_PENDING to 0),
                    null,
                    null
                )
                Timber.v("Marked $uri as not pending")
            }
        }

        return uri
    }

    /**
     * Retrieves the list of messages which should be shown in the notification
     * for a given conversation
     */
    override fun getUnreadUnseenMessages(threadId: Long): RealmResults<Message> {
        return Realm.getDefaultInstance().use {
            it.refresh()
            it.where(Message::class.java)
                .equalTo("seen", false)
                .equalTo("read", false)
                .equalTo("threadId", threadId)
                .sort("date")
                .findAll()
        }
    }

    override fun getUnreadMessages(threadId: Long): RealmResults<Message> {
        return Realm.getDefaultInstance().use {
            it.where(Message::class.java)
                .equalTo("read", false)
                .equalTo("threadId", threadId)
                .sort("date")
                .findAll()
        }
    }

    override fun markAllSeen() {
        Realm.getDefaultInstance().use {
            it.executeTransaction {
                it.where(Message::class.java)
                    .equalTo("seen", false)
                    .findAll()
                    .forEach { it.seen = true }
            }
        }
    }

    override fun markSeen(threadId: Long) {
        Realm.getDefaultInstance().use {
            it.executeTransaction {
                it.where(Message::class.java)
                    .equalTo("threadId", threadId)
                    .equalTo("seen", false)
                    .findAll()
                    .forEach { it.seen = true }
            }
        }
    }

    override fun markRead(vararg threadIds: Long) {
        Realm.getDefaultInstance().use { realm ->
            val messages = realm.where(Message::class.java)
                    .anyOf("threadId", threadIds)
                    .beginGroup()
                    .equalTo("read", false)
                    .or()
                    .equalTo("seen", false)
                    .endGroup()
                    .findAll()

            realm.executeTransaction {
                messages.forEach { message ->
                    message.seen = true
                    message.read = true
                }
            }
        }

        val values = ContentValues()
        values.put(Sms.SEEN, true)
        values.put(Sms.READ, true)

        threadIds.forEach {
            try {
                context.contentResolver.update(
                    ContentUris.withAppendedId(Telephony.MmsSms.CONTENT_CONVERSATIONS_URI, it),
                    values,
                    "${Sms.READ} = 0",
                    null
                )
            } catch (exception: Exception) {
                Timber.e(exception)
            }
        }
    }

    override fun markUnread(vararg threadIds: Long) {
        Realm.getDefaultInstance().use {
            val conversations = it.where(Conversation::class.java)
                .anyOf("id", threadIds)
                .equalTo("lastMessage.read", true)
                .findAll()

            it.executeTransaction {
                conversations.forEach { it.lastMessage?.read = false }
            }
        }
    }

    override fun sendMessage(
        subId: Int,
        threadId: Long,
        addresses: List<String>,
        body: String,
        attachments: List<Attachment>,
        delay: Int
    ) {
        val signedBody = when {
            prefs.signature.get().isEmpty() -> body
            body.isNotEmpty() -> body + '\n' + prefs.signature.get()
            else -> prefs.signature.get()
        }

        val smsManager = subId.takeIf { it != -1 }
                ?.let(SmsManagerFactory::createSmsManager)
                ?: SmsManager.getDefault()

        // We only care about stripping SMS
        val strippedBody = when (prefs.unicode.get()) {
            true -> StripAccents.stripAccents(signedBody)
            false -> signedBody
        }

        val smsParts = smsManager.divideMessage(strippedBody).orEmpty()
        val forceMms = (prefs.longAsMms.get() && (smsParts.size > 1))

        // if send as sms
        if ((addresses.size == 1) && attachments.isEmpty() && !forceMms) {
            val sendTime =
                if (delay > 0) (System.currentTimeMillis() + delay)
                else now()

            val message = insertSentSms(
                subId,
                threadId,
                addresses.first(),
                strippedBody,
                sendTime
            )

            if (delay == 0)    // no delay
                sendSms(message)
            else    // with delay
                (context.getSystemService(Context.ALARM_SERVICE) as AlarmManager)
                    .setExactAndAllowWhileIdle(
                        AlarmManager.RTC_WAKEUP,
                        sendTime,
                        getIntentForDelayedSms(message.id)
                    )

            return
        }

        // send as mms
        val parts = arrayListOf<MMSPart>()

        val maxWidth = smsManager
            .carrierConfigValues
            .getInt(SmsManager.MMS_CONFIG_MAX_IMAGE_WIDTH)
            .takeIf { prefs.mmsSize.get() == -1 }
            ?: Int.MAX_VALUE

        val maxHeight = smsManager
            .carrierConfigValues
            .getInt(SmsManager.MMS_CONFIG_MAX_IMAGE_HEIGHT)
            .takeIf { prefs.mmsSize.get() == -1 }
            ?: Int.MAX_VALUE

        var remainingBytes = when (prefs.mmsSize.get()) {
            -1 -> smsManager.carrierConfigValues.getInt(SmsManager.MMS_CONFIG_MAX_MESSAGE_SIZE)
            0 -> Int.MAX_VALUE
            else -> (prefs.mmsSize.get() * 1024)
        } * 0.95 // Ugly, but buys us a bit of wiggle room

        signedBody.takeIf { it.isNotEmpty() }?.toByteArray()?.let {
            remainingBytes -= it.size
            parts += MMSPart("text", ContentType.TEXT_PLAIN, it)
        }

        // Attach those that can't be compressed (ie. everything but images)
        parts += attachments
            // filter in non-images only
            .filter { !it.isImage(context) }
            // filter in only items that exist (user may have deleted the file)
            .filter { it.uri.resourceExists(context) }
            .map {
                remainingBytes -= it.getResourceBytes(context).size

                val mmsPart = MMSPart(
                    it.getName(context),
                    it.getType(context),
                    it.getResourceBytes(context)
                )

                // release the attachment hold on the image bytes so the GC can reclaim
                it.releaseResourceBytes()

                mmsPart
            }

        // get only images that have data available (user may have deleted the file)
        val images = attachments
            .filter { it.isImage(context) && it.uri.resourceExists(context) }
            .toMutableList()

        parts += images
            .map {
                var imageBytes = it.uri.getResourceBytes(context)

                val remainingImageBytes = images.sumOf { it.getResourceBytes(context).size }

                // if attachments need compression to fit into available mms space
                if (remainingImageBytes > remainingBytes) {
                    // prime the image resizer
                    val imageResizer = ImageUtils(
                        context,
                        it.uri.getResourceBytes(context),
                        maxWidth, maxHeight,
                        ((imageBytes.size /
                                remainingImageBytes.toFloat()) * remainingBytes).toInt(),
                        UNSPECIFIED,    // max starting quality (use default)
                        UNSPECIFIED,    // resize attempts (use default)
                    )

                    val scaledBytes = when (it.getType(context)) {
                        "image/gif" -> imageResizer.resizeGif()
                        else -> imageResizer.resizeImage()
                    }

                    if (scaledBytes == null)
                        Timber.d("Failed to resize image ${it.getName(context)}")

                    imageBytes = scaledBytes ?: it.getResourceBytes(context)
                }

                // adjust remaining space
                remainingBytes -= imageBytes.size

                val part = MMSPart(it.getName(context), it.getType(context), imageBytes)

                // release the attachment hold on the image bytes so the GC can reclaim
                it.releaseResourceBytes()

                part
            }

        // strip separators from outgoing mms, or they'll appear to have sent and not go through
        Transaction(context).sendNewMessage(
            subId,
            threadId,
            addresses.map(phoneNumberUtils::normalizeNumber),
            parts,
            null,
            null
        )
    }

    override fun sendSms(message: Message) {
        val smsManager = message.subId.takeIf { it != -1 }
                ?.let(SmsManagerFactory::createSmsManager)
                ?: SmsManager.getDefault()

        val parts = smsManager.divideMessage(
            if (prefs.unicode.get()) StripAccents.stripAccents(message.body)
            else message.body
        ) ?: arrayListOf()

        val sentIntents = parts.map {
            PendingIntent.getBroadcast(
                context, message.id.toInt(),
                Intent(context, SmsSentReceiver::class.java).putExtra("id", message.id),
                PendingIntent.FLAG_UPDATE_CURRENT or PendingIntent.FLAG_IMMUTABLE
            )
        }

        val deliveredIntents = parts.map {
            if (prefs.delivery.get()) PendingIntent.getBroadcast(
                context,
                message.id.toInt(),
                Intent(context, SmsDeliveredReceiver::class.java).putExtra("id", message.id),
                PendingIntent.FLAG_UPDATE_CURRENT or PendingIntent.FLAG_IMMUTABLE
            )
            else null
        }

        try {
            smsManager.sendMultipartTextMessage(
                    message.address,
                    null,
                    parts,
                    ArrayList(sentIntents),
                    ArrayList(deliveredIntents)
            )
        } catch (e: IllegalArgumentException) {
            Timber.w(e, "Message body lengths: ${parts.map { it?.length }}")
            markFailed(message.id, Telephony.MmsSms.ERR_TYPE_GENERIC)
        }
    }

    override fun resendMms(message: Message) {
        val subId = message.subId
        val threadId = message.threadId
        val pdu = tryOrNull {
            PduPersister.getPduPersister(context).load(message.getUri()) as MultimediaMessagePdu
        } ?: return

        val parts = message.parts.mapNotNull {
            val bytes = tryOrNull(false) {
                context.contentResolver.openInputStream(it.getUri())?.use {
                    inputStream -> inputStream.readBytes()
                }
            } ?: return@mapNotNull null

            MMSPart(it.name.orEmpty(), it.type, bytes)
        }

        Transaction(context).sendNewMessage(
            subId,
            threadId,
            pdu.to.map { it.string }.filter { it.isNotBlank() },
            parts,
            message.subject,
            message.getUri()
        )
    }

    override fun cancelDelayedSms(id: Long) {
        val alarmManager = context.getSystemService(Context.ALARM_SERVICE) as AlarmManager
        alarmManager.cancel(getIntentForDelayedSms(id))
    }

    private fun getIntentForDelayedSms(id: Long): PendingIntent {
        return PendingIntent.getBroadcast(
            context,
            id.toInt(),
            Intent(context, SendSmsReceiver::class.java).putExtra("id", id),
            PendingIntent.FLAG_UPDATE_CURRENT or PendingIntent.FLAG_IMMUTABLE
        )
    }

    override fun insertSentSms(
        subId: Int,
        threadId: Long,
        address: String,
        body: String,
        date: Long
    ): Message {
        // Insert the message to Realm
        val message = Message().apply {
            this.threadId = threadId
            this.address = address
            this.body = body
            this.date = date
            this.subId = subId

            id = messageIds.newId()
            boxId = Sms.MESSAGE_TYPE_OUTBOX
            type = "sms"
            read = true
            seen = true
        }

        Realm.getDefaultInstance().use { realm ->
            var managedMessage: Message? = null
            realm.executeTransaction { managedMessage = realm.copyToRealmOrUpdate(message) }

            // Insert the message to the native content provider
            val values = contentValuesOf(
                Sms.ADDRESS to address,
                Sms.BODY to body,
                Sms.DATE to System.currentTimeMillis(),
                Sms.READ to true,
                Sms.SEEN to true,
                Sms.TYPE to Sms.MESSAGE_TYPE_OUTBOX,
                Sms.THREAD_ID to threadId
            )

            if (prefs.canUseSubId.get()) {
                values.put(Sms.SUBSCRIPTION_ID, message.subId)
            }

            val uri = context.contentResolver.insert(Sms.CONTENT_URI, values)

            // Update the contentId after the message has been inserted to the content provider
            // The message might have been deleted by now, so only proceed if it's valid
            //
            // We do this after inserting the message because it might be slow, and we want the message
            // to be inserted into Realm immediately. We don't need to do this after receiving one
            uri?.lastPathSegment?.toLong()?.let { id ->
                realm.executeTransaction { managedMessage?.takeIf { it.isValid }?.contentId = id }
            }

            // On some devices, we can't obtain a threadId until after the first message is sent in a
            // conversation. In this case, we need to update the message's threadId after it gets added
            // to the native ContentProvider
            if (threadId == 0L) {
                uri?.let(syncRepository::syncMessage)
            }
        }

        return message
    }

    override fun insertReceivedSms(
        subId: Int,
        address: String,
        body: String,
        sentTime: Long
    ): Message {
        // Insert the message to Realm
        val message = Message().apply {
            this.address = address
            this.body = body
            this.dateSent = sentTime
            this.date = System.currentTimeMillis()
            this.subId = subId

            id = messageIds.newId()
            threadId = TelephonyCompat.getOrCreateThreadId(context, address)
            boxId = Sms.MESSAGE_TYPE_INBOX
            type = "sms"
            read = activeConversationManager.getActiveConversation() == threadId
        }

        Realm.getDefaultInstance().use { realm ->
            var managedMessage: Message? = null
            realm.executeTransaction { managedMessage = realm.copyToRealmOrUpdate(message) }

            // Insert the message to the native content provider
            val values = contentValuesOf(
                Sms.ADDRESS to address,
                Sms.BODY to body,
                Sms.DATE_SENT to sentTime,
            )

            if (prefs.canUseSubId.get()) {
                values.put(Sms.SUBSCRIPTION_ID, message.subId)
            }

            context.contentResolver.insert(Sms.Inbox.CONTENT_URI, values)
                ?.lastPathSegment
                ?.toLong()
                ?.let { id ->
                    // Update the contentId after the message has been inserted to the content provider
                    realm.executeTransaction { managedMessage?.contentId = id }
                }
        }

        return message
    }

    /**
     * Marks the message as sending, in case we need to retry sending it
     */
    override fun markSending(id: Long) {
        Realm.getDefaultInstance().use { realm ->
            realm.refresh()

            val message = realm.where(Message::class.java).equalTo("id", id).findFirst()
            message?.let {
                // Update the message in realm
                realm.executeTransaction {
                    message.boxId = when (message.isSms()) {
                        true -> Sms.MESSAGE_TYPE_OUTBOX
                        false -> Mms.MESSAGE_BOX_OUTBOX
                    }
                }

                // Update the message in the native ContentProvider
                context.contentResolver.update(
                    message.getUri(),
                    when (message.isSms()) {
                        true -> contentValuesOf(Sms.TYPE to Sms.MESSAGE_TYPE_OUTBOX)
                        false -> contentValuesOf(Mms.MESSAGE_BOX to Mms.MESSAGE_BOX_OUTBOX)
                    },
                    null,
                    null
                )
            }
        }
    }

    override fun markSent(id: Long) {
        Realm.getDefaultInstance().use { realm ->
            realm.refresh()

            val message = realm.where(Message::class.java).equalTo("id", id).findFirst()
            message?.let {
                // Update the message in realm
                realm.executeTransaction { message.boxId = Sms.MESSAGE_TYPE_SENT }

                // Update the message in the native ContentProvider

                context.contentResolver.update(
                    message.getUri(),
                    contentValuesOf(Sms.TYPE to Sms.MESSAGE_TYPE_SENT),
                    null,
                    null
                )
            }
        }
    }

    override fun markFailed(id: Long, resultCode: Int) {
        Realm.getDefaultInstance().use { realm ->
            realm.refresh()

            val message = realm.where(Message::class.java).equalTo("id", id).findFirst()
            message?.let {
                // Update the message in realm
                realm.executeTransaction {
                    message.boxId = Sms.MESSAGE_TYPE_FAILED
                    message.errorCode = resultCode
                }

                // Update the message in the native ContentProvider
                context.contentResolver.update(
                    message.getUri(),
                    contentValuesOf(
                        Sms.TYPE to Sms.MESSAGE_TYPE_FAILED,
                        Sms.ERROR_CODE to resultCode,
                    ),
                    null,
                    null
                )
            }
        }
    }

    override fun markDelivered(id: Long) {
        Realm.getDefaultInstance().use { realm ->
            realm.refresh()

            val message = realm.where(Message::class.java).equalTo("id", id).findFirst()
            message?.let {
                // Update the message in realm
                realm.executeTransaction {
                    message.deliveryStatus = Sms.STATUS_COMPLETE
                    message.dateSent = System.currentTimeMillis()
                    message.read = true
                }

                // Update the message in the native ContentProvider
                context.contentResolver.update(
                    message.getUri(),
                    contentValuesOf(
                        Sms.STATUS to Sms.STATUS_COMPLETE,
                        Sms.DATE_SENT to System.currentTimeMillis(),
                        Sms.READ to true,
                    ),
                    null,
                null
                )
            }
        }
    }

    override fun markDeliveryFailed(id: Long, resultCode: Int) {
        Realm.getDefaultInstance().use { realm ->
            realm.refresh()

            val message = realm.where(Message::class.java).equalTo("id", id).findFirst()
            message?.let {
                // Update the message in realm
                realm.executeTransaction {
                    message.deliveryStatus = Sms.STATUS_FAILED
                    message.dateSent = System.currentTimeMillis()
                    message.read = true
                    message.errorCode = resultCode
                }

                // Update the message in the native ContentProvider
                context.contentResolver.update(
                    message.getUri(),
                    contentValuesOf(
                        Sms.STATUS to Sms.STATUS_FAILED,
                        Sms.DATE_SENT to System.currentTimeMillis(),
                        Sms.READ to true,
                        Sms.ERROR_CODE to resultCode,
                    ),
                    null,
                    null
                )
            }
        }
    }

    override fun deleteMessages(vararg messageIds: Long) {
        Realm.getDefaultInstance().use { realm ->
            realm.refresh()

            val messages = realm.where(Message::class.java)
                    .anyOf("id", messageIds)
                    .findAll()

            val uris = messages.map { it.getUri() }

            realm.executeTransaction { messages.deleteAllFromRealm() }

            uris.forEach {
                uri -> context.contentResolver.delete(uri, null, null)
            }
        }
    }

    override fun getOldMessageCounts(maxAgeDays: Int): Map<Long, Int> {
        return Realm.getDefaultInstance().use { realm ->
            realm.where(Message::class.java)
                    .lessThan("date", now() - TimeUnit.DAYS.toMillis(maxAgeDays.toLong()))
                    .findAll()
                    .groupingBy { message -> message.threadId }
                    .eachCount()
        }
    }

    override fun deleteOldMessages(maxAgeDays: Int) {
        return Realm.getDefaultInstance().use { realm ->
            val messages = realm.where(Message::class.java)
                    .lessThan("date", now() - TimeUnit.DAYS.toMillis(maxAgeDays.toLong()))
                    .findAll()

            val uris = messages.map { it.getUri() }

            realm.executeTransaction { messages.deleteAllFromRealm() }

            uris.forEach { uri -> context.contentResolver.delete(uri, null, null) }
        }
    }
}
