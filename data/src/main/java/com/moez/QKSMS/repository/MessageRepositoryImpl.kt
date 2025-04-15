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

import com.moez.QKSMS.manager.QkTransaction
import android.app.AlarmManager
import android.app.PendingIntent
import android.content.ContentUris
import android.content.Context
import android.content.Intent
import android.graphics.BitmapFactory
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
import com.klinker.android.send_message.Message.Part
import com.klinker.android.send_message.Settings
import com.klinker.android.send_message.SmsManagerFactory
import dev.octoshrimpy.quik.common.util.extensions.now
import dev.octoshrimpy.quik.compat.TelephonyCompat
import dev.octoshrimpy.quik.extensions.anyOf
import dev.octoshrimpy.quik.extensions.getResourceBytes
import dev.octoshrimpy.quik.extensions.insertOrUpdate
import dev.octoshrimpy.quik.extensions.isImage
import dev.octoshrimpy.quik.extensions.isSmil
import dev.octoshrimpy.quik.extensions.isText
import dev.octoshrimpy.quik.extensions.isVideo
import dev.octoshrimpy.quik.extensions.map
import dev.octoshrimpy.quik.extensions.resourceExists
import dev.octoshrimpy.quik.manager.ActiveConversationManager
import dev.octoshrimpy.quik.manager.KeyManager
import dev.octoshrimpy.quik.mapper.CursorToMessage
import dev.octoshrimpy.quik.mapper.CursorToPart
import dev.octoshrimpy.quik.model.Attachment
import dev.octoshrimpy.quik.model.Conversation
import dev.octoshrimpy.quik.model.Message
import dev.octoshrimpy.quik.model.Message.Companion.TYPE_MMS
import dev.octoshrimpy.quik.model.Message.Companion.TYPE_SMS
import dev.octoshrimpy.quik.model.MmsPart
import dev.octoshrimpy.quik.receiver.MessageDeliveredReceiver
import dev.octoshrimpy.quik.receiver.MessageSentReceiver
import dev.octoshrimpy.quik.receiver.SendDelayedMessageReceiver
import dev.octoshrimpy.quik.receiver.SendDelayedMessageReceiver.Companion.MESSAGE_ID_EXTRA
import dev.octoshrimpy.quik.repository.MessageRepository.SendNewMessagesType
import dev.octoshrimpy.quik.util.ImageUtils
import dev.octoshrimpy.quik.util.PhoneNumberUtils
import dev.octoshrimpy.quik.util.Preferences
import dev.octoshrimpy.quik.util.tryOrNull
import io.realm.Case
import io.realm.Realm
import io.realm.RealmList
import io.realm.RealmResults
import io.realm.Sort
import timber.log.Timber
import java.util.concurrent.TimeUnit
import javax.inject.Inject
import javax.inject.Singleton
import kotlin.math.sqrt

@Singleton
open class MessageRepositoryImpl @Inject constructor(
    private val activeConversationManager: ActiveConversationManager,
    private val context: Context,
    private val messageIds: KeyManager,
    private val phoneNumberUtils: PhoneNumberUtils,
    private val prefs: Preferences,
    private val cursorToMessage: CursorToMessage,
    private val cursorToPart: CursorToPart,
) : MessageRepository {

    companion object {
        const val TELEPHONY_UPDATE_CHUNK_SIZE = 200
    }

    private fun getMessagesBase(threadId: Long, query: String) =
        Realm.getDefaultInstance()
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

    override fun getMessages(threadId: Long, query: String): RealmResults<Message> =
        getMessagesBase(threadId, query).findAllAsync()

    override fun getMessagesSync(threadId: Long, query: String): RealmResults<Message> =
        getMessagesBase(threadId, query).findAll()

    override fun getMessage(messageId: Long) =
        Realm.getDefaultInstance()
            .also { it.refresh() }
            .where(Message::class.java)
            .equalTo("id", messageId)
            .findFirst()

    override fun getUnmanagedMessage(messageId: Long) =
        Realm.getDefaultInstance().use { realm ->
            getMessage(messageId)?.let(realm::copyFromRealm)
        }

    override fun getMessages(messageIds: Collection<Long>): RealmResults<Message> =
        Realm.getDefaultInstance()
            .also { it.refresh() }
            .where(Message::class.java)
            .anyOf("id", messageIds.toLongArray())
            .findAll()

    override fun getMessageForPart(id: Long) =
        Realm.getDefaultInstance()
            .where(Message::class.java)
            .equalTo("parts.id", id)
            .findFirst()

    override fun getLastIncomingMessage(threadId: Long): RealmResults<Message> =
        Realm.getDefaultInstance()
            .where(Message::class.java)
            .equalTo("threadId", threadId)
            .beginGroup()
            .beginGroup()
            .equalTo("type", TYPE_SMS)
            .`in`("boxId", arrayOf(Sms.MESSAGE_TYPE_INBOX, Sms.MESSAGE_TYPE_ALL))
            .endGroup()
            .or()
            .beginGroup()
            .equalTo("type", TYPE_MMS)
            .`in`("boxId", arrayOf(Mms.MESSAGE_BOX_INBOX, Mms.MESSAGE_BOX_ALL))
            .endGroup()
            .endGroup()
            .sort("date", Sort.DESCENDING)
            .findAll()

    override fun getUnreadCount() =
        Realm.getDefaultInstance().use { realm ->
            realm.refresh()
            realm.where(Conversation::class.java)
                .equalTo("archived", false)
                .equalTo("blocked", false)
                .equalTo("lastMessage.read", false)
                .count()
        }

    override fun getPart(id: Long) =
        Realm.getDefaultInstance()
            .where(MmsPart::class.java)
            .equalTo("id", id)
            .findFirst()

    override fun getPartsForConversation(threadId: Long): RealmResults<MmsPart> =
        Realm.getDefaultInstance()
            .where(MmsPart::class.java)
            .equalTo("messages.threadId", threadId)
            .beginGroup()
            .contains("type", "image/")
            .or()
            .contains("type", "video/")
            .endGroup()
            .sort("id", Sort.DESCENDING)
            .findAllAsync()

    override fun savePart(id: Long): Uri? {
        val part = getPart(id) ?: return null

        val extension = MimeTypeMap.getSingleton().getExtensionFromMimeType(part.type)
            ?: return null
        val date = part.messages?.first()?.date
        val fileName = part.name?.takeIf { name -> name.endsWith(extension) }
            ?: "${part.type.split("/").last()}_$date.$extension"

        val values = contentValuesOf(
            MediaStore.MediaColumns.DISPLAY_NAME to fileName,
            MediaStore.MediaColumns.MIME_TYPE to part.type,
        )

        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.Q) {
            values.put(MediaStore.MediaColumns.IS_PENDING, 1)
            values.put(
                MediaStore.MediaColumns.RELATIVE_PATH, when {
                    part.isImage() -> "${Environment.DIRECTORY_PICTURES}/QUIK"
                    part.isVideo() -> "${Environment.DIRECTORY_MOVIES}/QUIK"
                    else -> "${Environment.DIRECTORY_DOWNLOADS}/QUIK"
                }
            )
        }

        val contentUri = when {
            part.isImage() -> MediaStore.Images.Media.EXTERNAL_CONTENT_URI
            part.isVideo() -> MediaStore.Video.Media.EXTERNAL_CONTENT_URI
            Build.VERSION.SDK_INT >= Build.VERSION_CODES.Q ->
                MediaStore.Downloads.EXTERNAL_CONTENT_URI
            else -> MediaStore.Files.getContentUri("external")
        }

        val uri = context.contentResolver.insert(contentUri, values)
        Timber.v("Saving $fileName (${part.type}) to $uri")

        uri?.let {
            context.contentResolver.openOutputStream(uri)?.use { outputStream ->
                context.contentResolver.openInputStream(part.getUri())?.use { inputStream ->
                    inputStream.copyTo(outputStream, 1024)
                }
            }
            Timber.v("Saved $fileName (${part.type}) to $uri")

            if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.Q) {
                context.contentResolver.update(
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

    override fun getUnreadUnseenMessages(threadId: Long): RealmResults<Message> =
        Realm.getDefaultInstance()
            .also { it.refresh() }
            .where(Message::class.java)
            .equalTo("seen", false)
            .equalTo("read", false)
            .equalTo("threadId", threadId)
            .sort("date")
            .findAll()

    override fun getUnreadMessages(threadId: Long): RealmResults<Message> =
        Realm.getDefaultInstance()
            .where(Message::class.java)
            .equalTo("read", false)
            .equalTo("threadId", threadId)
            .sort("date")
            .findAll()

    // marks all messages in threads as read and/or seen in the native provider
    private fun telephonyMarkSeenRead(
        seen: Boolean?,
        read: Boolean?,
        threadIds: Collection<Long>,
    ): Int {
        if (((seen == null) && (read == null)) || threadIds.isEmpty())
            return -1

        var countUpdated = 0

        // 'read' can be modified at the conversation level which updates all messages
        read?.let {
            tryOrNull(true) {
                // chunked so where clause doesn't get too long if there are many threads
                threadIds.forEach {
                    countUpdated += context.contentResolver.update(
                        ContentUris.withAppendedId(
                            Telephony.MmsSms.CONTENT_CONVERSATIONS_URI,
                            it
                        ),
                        contentValuesOf(Sms.READ to read),
                        "${Sms.READ} = ${if (read) 0 else 1}",
                        null
                    )
                }
            }
        }

        seen?.let {
            // 'seen' has to be modified at the messages level
            threadIds.chunked(TELEPHONY_UPDATE_CHUNK_SIZE).forEach {
                // chunked for smaller where clause size
                val values = contentValuesOf(Sms.SEEN to seen)
                val whereClause ="${Sms.SEEN} = ${if (seen) 0 else 1} " +
                        "and ${Sms.THREAD_ID} in (${it.joinToString(",")})"

                // sms messages
                tryOrNull(true) {
                    countUpdated += context.contentResolver.update(
                        Sms.CONTENT_URI, values, whereClause, null
                    )
                }

                // mms messages
                tryOrNull(true) {
                    countUpdated += context.contentResolver.update(
                        Mms.CONTENT_URI, values, whereClause, null
                    )
                }
            }
        }

        return countUpdated  // a mix of convo and message updates, so not overly useful. meh
    }

    override fun markAllSeen() =
        mutableSetOf<Long>().let { threadIds ->
            Realm.getDefaultInstance().use { realm ->
                realm.where(Message::class.java)
                    .equalTo("seen", false)
                    .findAll()
                    .takeIf { it.isNotEmpty() }
                    ?.let { messages ->
                        realm.executeTransaction {
                            messages.forEach {
                                it.seen = true
                                threadIds += it.threadId
                            }
                        }
                    }
            }.run {
                telephonyMarkSeenRead(true, null, threadIds)
            }
        }

    override fun markSeen(threadIds: Collection<Long>) =
        Realm.getDefaultInstance().use { realm ->
            realm.where(Message::class.java)
                .anyOf("threadId", threadIds.toLongArray())
                .equalTo("seen", false)
                .findAll()
                .let { messages ->
                    realm.executeTransaction {
                        messages.forEach { it.seen = true }
                    }
                }
        }.run {
            telephonyMarkSeenRead(true, null, threadIds)
        }

    override fun markRead(threadIds: Collection<Long>) =
        threadIds.takeIf { it.isNotEmpty() }
            ?.let {
                Realm.getDefaultInstance()?.use { realm ->
                    realm.where(Message::class.java)
                        .anyOf("threadId", threadIds.toLongArray())
                        .beginGroup()
                        .equalTo("read", false)
                        .or()
                        .equalTo("seen", false)
                        .endGroup()
                        .findAll()
                        .let { messages ->
                            realm.executeTransaction {
                                messages.forEach { it.seen = true; it.read = true }
                            }
                        }
                }.run {
                    telephonyMarkSeenRead(true, true, threadIds)
                }
            }
            ?: 0

    override fun markUnread(threadIds: Collection<Long>) =
        threadIds.takeIf { it.isNotEmpty() }
            ?.let {
                Realm.getDefaultInstance()?.use { realm ->
                    val conversations = realm.where(Conversation::class.java)
                        .anyOf("id", threadIds.toLongArray())
                        .equalTo("lastMessage.read", true)
                        .findAll()

                    realm.executeTransaction {
                        conversations.forEach { it.lastMessage?.read = false }
                    }
                }.run {
                    telephonyMarkSeenRead(null, false, threadIds)
                }
            }
            ?: 0

    private fun syncProviderMessage(uri: Uri, sendAsGroup: Boolean): Message? {
        // if uri doesn't have valid type
        val type = when {
            uri.toString().contains(Message.TYPE_MMS) -> TYPE_MMS
            uri.toString().contains(Message.TYPE_SMS) -> TYPE_SMS
            else -> return null
        }

        // if uri doesn't have a valid id, fail
        val contentId = tryOrNull(false) { ContentUris.parseId(uri) } ?: return null

        val stableUri = when (type) {
            TYPE_MMS -> ContentUris.withAppendedId(Mms.CONTENT_URI, contentId)
            else -> ContentUris.withAppendedId(Sms.CONTENT_URI, contentId)
        }

        return context.contentResolver.query(
            stableUri, null, null, null, null
        )?.use { cursor ->
            // if there are no rows, return null. else, move to the first row
            if (!cursor.moveToFirst())
                return null

            cursorToMessage.map(Pair(cursor, CursorToMessage.MessageColumns(cursor))).apply {
                this.sendAsGroup = sendAsGroup

                if (isMms()) {
                    parts = RealmList<MmsPart>().apply {
                        addAll(
                            cursorToPart.getPartsCursor(contentId)
                                ?.map { cursorToPart.map(it) }
                                .orEmpty()
                        )
                    }
                }

                insertOrUpdate()
            }
        }
    }

    override fun sendNewMessages(
        subId: Int, toAddresses: Collection<String>, body: String,
        attachments: Collection<Attachment>, sendAsGroup: Boolean, delayMs: Int,
        messageTypes: SendNewMessagesType
    ): Collection<Message> {
        Timber.v("sending message(s)")

        val parts = mutableListOf<Part>()

        if (attachments.isNotEmpty()) {
            Timber.v("has attachments")
            val smsManager = subId.takeIf { it != -1 }
                ?.let(SmsManagerFactory::createSmsManager)
                ?: SmsManager.getDefault()

            val maxWidth = smsManager.carrierConfigValues
                .getInt(SmsManager.MMS_CONFIG_MAX_IMAGE_WIDTH)
                .takeIf { prefs.mmsSize.get() == -1 }
                ?: Int.MAX_VALUE

            val maxHeight = smsManager.carrierConfigValues
                .getInt(SmsManager.MMS_CONFIG_MAX_IMAGE_HEIGHT)
                .takeIf { prefs.mmsSize.get() == -1 }
                ?: Int.MAX_VALUE

            var remainingBytes = when (prefs.mmsSize.get()) {
                -1 -> smsManager.carrierConfigValues.getInt(SmsManager.MMS_CONFIG_MAX_MESSAGE_SIZE)
                0 -> Int.MAX_VALUE
                else -> prefs.mmsSize.get() * 1024
            } * 0.9 // Ugly, but buys us a bit of wiggle room

            remainingBytes -= body.takeIf { it.isNotEmpty() }?.toByteArray()?.size ?: 0

            // Attach those that can't be compressed (ie. everything but images)
            parts += attachments
                // filter in non-images only
                .filter { !it.isImage(context) }
                // filter in only items that exist (user may have deleted the file)
                .filter { it.uri.resourceExists(context) }
                .map {
                    remainingBytes -= it.getResourceBytes(context).size
                    val part = Part(
                        it.getResourceBytes(context), it.getType(context), it.getName(context)
                    )

                    // release the attachment hold on the image bytes so the GC can reclaim
                    it.releaseResourceBytes()

                    part
                }

            val imageBytesByAttachment = attachments
                // filter in images only
                .filter { it.isImage(context) }
                // filter in only items that exist (user may have deleted the file)
                .filter { it.uri.resourceExists(context) }
                .associateWith {
                    when (it.getType(context) == "image/gif") {
                        true -> ImageUtils.getScaledGif(context, it.uri, maxWidth, maxHeight)
                        false -> ImageUtils.getScaledImage(context, it.uri, maxWidth, maxHeight)
                    }
                }
                .toMutableMap()

            val imageByteCount = imageBytesByAttachment.values.sumOf { it.size }
            if (imageByteCount > remainingBytes) {
                imageBytesByAttachment.forEach { (attachment, originalBytes) ->
                    val uri = attachment.uri
                    val maxBytes = originalBytes.size / imageByteCount.toFloat() * remainingBytes

                    // Get the image dimensions
                    val options = BitmapFactory.Options().apply { inJustDecodeBounds = true }
                    BitmapFactory.decodeStream(
                        context.contentResolver.openInputStream(uri),
                        null,
                        options
                    )
                    val width = options.outWidth
                    val height = options.outHeight
                    val aspectRatio = width.toFloat() / height.toFloat()

                    var attempts = 0
                    var scaledBytes = originalBytes

                    while (scaledBytes.size > maxBytes) {
                        // Estimate how much we need to scale the image down by. If it's still
                        // too big, we'll need to try smaller and smaller values
                        val scale = maxBytes / originalBytes.size * (0.9 - attempts * 0.2)
                        if (scale <= 0) {
                            Timber.w(
                                "Failed to compress ${
                                    originalBytes.size / 1024
                                }Kb to ${maxBytes.toInt() / 1024}Kb"
                            )
                            return@forEach
                        }

                        val newArea = scale * width * height
                        val newWidth = sqrt(newArea * aspectRatio).toInt()
                        val newHeight = (newWidth / aspectRatio).toInt()

                        attempts++
                        scaledBytes = when (attachment.getType(context) == "image/gif") {
                            true -> ImageUtils.getScaledGif(
                                context, attachment.uri, newWidth, newHeight
                            )

                            false -> ImageUtils.getScaledImage(
                                context, attachment.uri, newWidth, newHeight
                            )
                        }

                        Timber.d(
                            "Compression attempt $attempts: ${
                                scaledBytes.size / 1024
                            }/${maxBytes.toInt() / 1024}Kb ($width*$height -> $newWidth*${
                                newHeight
                            })"
                        )

                        // release the attachment hold on the image bytes so the GC can reclaim
                        attachment.releaseResourceBytes()
                    }

                    Timber.v(
                        "Compressed ${originalBytes.size / 1024}Kb to ${
                            scaledBytes.size / 1024
                        }Kb with a target size of ${
                            maxBytes.toInt() / 1024
                        }Kb in $attempts attempts"
                    )
                    imageBytesByAttachment[attachment] = scaledBytes
                }
            }

            imageBytesByAttachment.forEach { (attachment, bytes) ->
                parts += Part(
                    bytes,
                    if (attachment.getType(context) == "image/gif") ContentType.IMAGE_GIF
                    else ContentType.IMAGE_JPEG,
                    attachment.getName(context)
                )
            }
        }

        Timber.v("create os provider messages")

        // 3 stage sending process - stage 1, create records in os provider
        val messages =
            (QkTransaction(
                context,
                Settings().apply {
                    useSystemSending = true
                    subscriptionId = subId
                    sendLongAsMms = prefs.longAsMms.get()
                    group = (sendAsGroup && (toAddresses.size > 1))
                    signature = prefs.signature.get()
                    stripUnicode = prefs.unicode.get()
                }
            )
                .sendNewMessage(
                    com.klinker.android.send_message.Message().apply {
                        text = body
                        addresses = toAddresses.map(phoneNumberUtils::normalizeNumber).toTypedArray()
                        parts.forEach { addMedia(it.media, it.contentType, it.name) }
                    },
                    QkTransaction.NO_THREAD_ID,
                    false,
//true,
                    if (delayMs > 0)  // if delaying, only get template message
                        QkTransaction.SendNewMessageType.TemplateMessageOnly
                    else if (messageTypes == SendNewMessagesType.NoTemplateMessage)
                        QkTransaction.SendNewMessageType.NoTemplateMessage
                    else
                        QkTransaction.SendNewMessageType.AllMessages
                )
                // stage 2 - create local db records for each created os provider record
                ?.filter { messageUri -> (messageUri != null) && (messageUri != Uri.EMPTY) }
                ?.mapNotNull { messageUri ->
                    syncProviderMessage(messageUri, sendAsGroup)
                        ?.let { message ->
                            Timber.v("created message id ${message.id} from uri $messageUri")
                            message
                        }
                        ?:let { Timber.e("message sync failed for uri $messageUri"); null }
                }
                ?:let { Timber.e("smsmms sendnewmessage create returned empty or null"); listOf() }
            ).toMutableList()

        if (delayMs <= 0) {  // not delay sending
            // stage 3 - send the messages
            messages.mapIndexed { index, message ->
                // if multiple messages returned, the first one is a 'group' message for other
                // messages that are to be sent individually, mark it sent and don't send it
                if ((messages.size > 1) && (index == 0))
                    markSent(messages[index].id)
                else
                    sendMessage(message, toAddresses)
            }
        } else {  // delay sending, take the first returned message (there should only be one)
            messages.firstOrNull()?.let { message ->
                val sendTime = (now() + delayMs)

                // set delay time on the db message
                Realm.getDefaultInstance().use { realm ->
                    realm.executeTransaction {
                        realm.copyToRealmOrUpdate(message.apply { date = sendTime })
                    }
                }

                // create alarm that will trigger sending the message
                (context.getSystemService(Context.ALARM_SERVICE) as AlarmManager)
                    .setExactAndAllowWhileIdle(
                        AlarmManager.RTC_WAKEUP, sendTime,
                        getIntentForDelayedSms(message.id)
                    )

                Timber.v("set ${delayMs}ms delay for message id ${message.id}")
            }
        }

        return messages
    }

    override fun sendNewMessagesFromTemplate(
        message: Message, toAddresses: Collection<String>, delayMs: Int
    ) =
        sendNewMessages(
            message.subId, toAddresses, message.getText(false),
            message.parts.map { Attachment(context, it.getUri()) },
            message.sendAsGroup,
            delayMs,
            SendNewMessagesType.NoTemplateMessage  // already have template message in db
        )

    override fun sendMessage(message: Message, toAddresses: Collection<String>) =
        tryOrNull(true) {
            markSending(message.id)

            val sentMessageIntent = Intent(context, MessageSentReceiver::class.java)
                .putExtra(MessageSentReceiver.MESSAGE_ID_EXTRA, message.id)

            val deliveredMessageIntent = Intent(context, MessageDeliveredReceiver::class.java)
                .putExtra(MessageDeliveredReceiver.MESSAGE_ID_EXTRA, message.id)

            // use values from os provider to resend the message, except subId
            QkTransaction(
                context,
                Settings().apply {
                    useSystemSending = true
                    subscriptionId = message.subId
                    sendLongAsMms = prefs.longAsMms.get()
                    group = message.sendAsGroup
                    stripUnicode = prefs.unicode.get()
                }
            )
                .setExplicitBroadcastForSentMms(sentMessageIntent)
                .setExplicitBroadcastForSentSms(sentMessageIntent)
                .setExplicitBroadcastForDeliveredSms(deliveredMessageIntent)
                .sendNewMessage(
                    com.klinker.android.send_message.Message().apply {
                        messageUri = message.getUri()
                        text = message.body
                        addresses = toAddresses.toTypedArray()
                        message.parts.forEach {
                            if (!it.isSmil())
                                addMedia(
                                    if (it.isText()) it.text?.toByteArray()
                                    else it.getUri().getResourceBytes(context),
                                    it.type,
                                    it.name
                                )
                        }
                    },
                    QkTransaction.NO_THREAD_ID,
                    true,
                    QkTransaction.SendNewMessageType.NoTemplateMessage
                )
                    ?.firstOrNull { messageUri -> (messageUri != null) && (messageUri != Uri.EMPTY) }
                    ?.let { true }
                    ?:let { Timber.e("message id ${message.id} not sent by smsmms");  false }
        } ?: false

    override fun sendMessage(messageId: Long, toAddresses: Collection<String>) =
        getMessage(messageId)
            ?.let { message -> sendMessage(message, toAddresses) }
            ?: false

    override fun cancelDelayedSmsAlarm(messageId: Long) =
        (context.getSystemService(Context.ALARM_SERVICE) as AlarmManager)
            .cancel(getIntentForDelayedSms(messageId))

    private fun getIntentForDelayedSms(messageId: Long) =
        PendingIntent.getBroadcast(
            context,
            messageId.toInt(),
            Intent(context, SendDelayedMessageReceiver::class.java)
                .putExtra(MESSAGE_ID_EXTRA, messageId),
            PendingIntent.FLAG_UPDATE_CURRENT or PendingIntent.FLAG_IMMUTABLE
        )

    override fun insertReceivedSms(subId: Int, address: String, body: String, sentTime: Long)
    : Message {
        val threadId = TelephonyCompat.getOrCreateThreadId(context, address)

        // insert the message to the native content provider
        val values = contentValuesOf(
            Sms.ADDRESS to address,
            Sms.BODY to body,
            Sms.DATE_SENT to sentTime,
            Sms.THREAD_ID to threadId
        )

        if (prefs.canUseSubId.get())
            values.put(Sms.SUBSCRIPTION_ID, subId)

        val providerContentId = context.contentResolver.insert(Sms.Inbox.CONTENT_URI, values)
            ?.let { insertedUri -> ContentUris.parseId(insertedUri) }
            ?: 0

        // insert the message to Realm
        val message = Message().apply {
            id = messageIds.newId()

            this.address = address
            this.body = body
            this.dateSent = sentTime
            this.threadId = threadId
            this.subId = subId

            date = System.currentTimeMillis()

            contentId = providerContentId
            boxId = Sms.MESSAGE_TYPE_INBOX
            type = TYPE_SMS
            read = (activeConversationManager.getActiveConversation() == threadId)
        }

        Realm.getDefaultInstance().use { realm ->
            realm.executeTransaction { realm.copyToRealmOrUpdate(message) }
        }

        return message
    }

    /**
     * Marks the message as sending, in case we need to retry sending it
     */
    override fun markSending(id: Long) =
        Realm.getDefaultInstance().use { realm ->
            realm.refresh()

            realm.where(Message::class.java)
                .equalTo("id", id)
                .findFirst()
                ?.let { message ->
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
            Unit
        }

    override fun markSent(messageId: Long) {
        Timber.v("mark message id ${messageId} as sent")

        Realm.getDefaultInstance().use { realm ->
            realm.refresh()

            realm.where(Message::class.java).equalTo("id", messageId).findFirst()
                ?.let { message ->
                    if (message.isSms()) {
                        // update the message in realm
                        realm.executeTransaction { message.boxId = Sms.MESSAGE_TYPE_SENT }

                        // Update the message in the native ContentProvider
                        context.contentResolver.update(
                            message.getUri(),
                            contentValuesOf(Sms.TYPE to Sms.MESSAGE_TYPE_SENT),
                            null,
                            null
                        )
                    } else {
                        // update the message in realm
                        realm.executeTransaction { message.boxId = Mms.MESSAGE_BOX_SENT }

                        // Update the message in the native ContentProvider
                        context.contentResolver.update(
                            message.getUri(),
                            contentValuesOf(Mms.MESSAGE_BOX to Mms.MESSAGE_BOX_SENT),
                            null,
                            null
                        )
                    }
                }
        }
    }

    override fun markFailed(messageId: Long, resultCode: Int) =
        Realm.getDefaultInstance().use { realm ->
            Timber.v("mark message id $messageId as failed. code $resultCode")

            realm.refresh()

            realm.where(Message::class.java).equalTo("id", messageId).findFirst()
                ?.let { message ->
                    if (message.isSms()) {
                        if (message.boxId != Sms.MESSAGE_TYPE_FAILED) {
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
                            true
                        } else false
                    } else {  // mms
                        if (message.boxId != Mms.MESSAGE_BOX_FAILED) {
                            // Update the message in realm
                            realm.executeTransaction {
                                message.boxId = Mms.MESSAGE_BOX_FAILED
                                message.errorCode = resultCode
                            }

                            // Update the message in the native ContentProvider
                            context.contentResolver.update(
                                message.getUri(),
                                contentValuesOf(
                                    Mms.MESSAGE_BOX to Mms.MESSAGE_BOX_FAILED
                                ),
                                null,
                                null
                            )

                            // TODO this query isn't able to find any results
                            // Need to figure out why the message isn't appearing in the PendingMessages Uri,
                            // so that we can properly assign the error type
                            context.contentResolver.update(
                                Telephony.MmsSms.PendingMessages.CONTENT_URI,
                                contentValuesOf(
                                    Telephony.MmsSms.PendingMessages.ERROR_TYPE to Telephony.MmsSms.ERR_TYPE_GENERIC_PERMANENT
                                ),
                                "${Telephony.MmsSms.PendingMessages.MSG_ID} = ?",
                                arrayOf(message.id.toString())
                            )
                            true
                        } else false
                    }
            } ?: false
        }

    override fun markDelivered(id: Long) =
        Realm.getDefaultInstance().use { realm ->
            realm.refresh()

            realm.where(Message::class.java)
                .equalTo("id", id)
                .findFirst()
                ?.let { message ->
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
            Unit
        }

    override fun markDeliveryFailed(id: Long, resultCode: Int) =
        Realm.getDefaultInstance().use { realm ->
            realm.refresh()

            realm.where(Message::class.java)
                .equalTo("id", id)
                .findFirst()
                ?.let { message ->
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
            Unit
        }

    override fun deleteMessages(messageIds: Collection<Long>) =
        Realm.getDefaultInstance().use { realm ->
            realm.refresh()

            realm.where(Message::class.java)
                .anyOf("id", messageIds.toLongArray())
                .findAll()
                ?.let { messages ->
                    messages.mapNotNull { message ->
                        val uri = message.getUri()
                        if (uri != Uri.EMPTY)
                            context.contentResolver.delete(uri, null, null)
                    }

                    realm.executeTransaction { messages.deleteAllFromRealm() }
                } ?: Unit
        }

    override fun getOldMessageCounts(maxAgeDays: Int) =
        Realm.getDefaultInstance().use { realm ->
            realm.where(Message::class.java)
                .lessThan(
                    "date",
                    now() - TimeUnit.DAYS.toMillis(maxAgeDays.toLong())
                )
                .findAll()
                .groupingBy { message -> message.threadId }
                .eachCount()
        }

    override fun deleteOldMessages(maxAgeDays: Int) =
        Realm.getDefaultInstance().use { realm ->
            val messages = realm.where(Message::class.java)
                .lessThan(
                    "date",
                    now() - TimeUnit.DAYS.toMillis(maxAgeDays.toLong())
                )
                .findAll()

            val uris = messages.map { it.getUri() }

            realm.executeTransaction { messages.deleteAllFromRealm() }

            uris.forEach {
                uri -> context.contentResolver.delete(uri, null, null)
            }
        }
}

