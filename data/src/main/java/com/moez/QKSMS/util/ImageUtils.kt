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
package dev.octoshrimpy.quik.util

import android.app.ActivityManager
import android.content.Context
import android.graphics.Bitmap
import android.graphics.BitmapFactory
import android.graphics.Matrix
import androidx.exifinterface.media.ExifInterface
import timber.log.Timber
import java.io.ByteArrayOutputStream
import java.io.FileNotFoundException
import java.io.IOException
import kotlin.math.max
import kotlin.math.min
import kotlin.math.sqrt

class ImageUtils(
    private val context: Context,
    private val imageBytes: ByteArray,
    private var widthLimit: Int,
    private val heightLimit: Int,
    private var byteLimit: Int,
    private var quality: Int,
    private var resizeAttempts: Int,
) {
    class OrientationParams {
        var rotation: Int = 0
        var scaleX: Int = 1
        var scaleY: Int = 1
        var invertDimensions: Boolean = false
    }

    interface Orientation {
        companion object {
            const val TOP_LEFT = 1
            const val TOP_RIGHT = 2
            const val BOTTOM_LEFT = 3
            const val BOTTOM_RIGHT = 4
            const val LEFT_TOP = 5
            const val RIGHT_TOP = 6
            const val LEFT_BOTTOM = 7
            const val RIGHT_BOTTOM = 8
        }
    }

    companion object {
        const val UNSPECIFIED = -1

        private const val MAX_OOM_COUNT = 1

        private const val DEFAULT_MAX_IMAGE_COMPRESSION_QUALITY = 95
        private const val MINIMUM_IMAGE_COMPRESSION_QUALITY = 50

        private const val DEFAULT_NUMBER_OF_RESIZE_ATTEMPTS = 6

        // minimum factor to reduce quality value
        private const val QUALITY_SCALE_DOWN_RATIO = 0.85

        // amount to scale down the picture when it doesn't fit
        private const val MIN_SCALE_DOWN_RATIO = 0.75f

        // when computing sampleSize target scaling of no more than this ratio
        private const val MAX_TARGET_SCALE_FACTOR = 1.5f

        private fun getOrientationParams(imageBytes: ByteArray): OrientationParams {
            val params = OrientationParams()
            val orientation = try {
                ExifInterface(imageBytes.inputStream())
                    .getAttribute(ExifInterface.TAG_ORIENTATION)
                    ?.toInt()
                    ?: ExifInterface.ORIENTATION_UNDEFINED
            } catch (e: Exception) {
                ExifInterface.ORIENTATION_UNDEFINED
            }

            when (orientation) {
                Orientation.TOP_RIGHT -> params.scaleX = -1
                Orientation.BOTTOM_RIGHT -> params.scaleY = -1
                Orientation.BOTTOM_LEFT -> params.rotation = 180
                Orientation.RIGHT_BOTTOM -> {
                    params.rotation = 270
                    params.invertDimensions = true
                }

                Orientation.RIGHT_TOP -> {
                    params.rotation = 90
                    params.invertDimensions = true
                }

                Orientation.LEFT_TOP -> {
                    params.rotation = 90
                    params.scaleX = -1
                    params.invertDimensions = true
                }

                Orientation.LEFT_BOTTOM -> {
                    params.rotation = 270
                    params.scaleX = -1
                    params.invertDimensions = true
                }
            }
            return params
        }

        // transforms a bitmap into a byte array
        fun bitmapToBytes(bitmap: Bitmap, quality: Int): ByteArray? {
            for (oomCount in 0 until MAX_OOM_COUNT) {
                try {
                    val os = ByteArrayOutputStream()
                    bitmap.compress(Bitmap.CompressFormat.JPEG, quality, os)
                    return os.toByteArray()
                } catch (e: OutOfMemoryError) {
                    Timber.w("OutOfMemory converting bitmap to bytes")
                }
            }

            Timber.w("Failed to convert bitmap to bytes. Out of Memory")
            return null
        }
    }

    private val width: Int
    private val height: Int

    private var sampleSize = 0

    private var decodedBitmap: Bitmap? = null
    private var scaledBitmap: Bitmap? = null

    private var scaleFactor = 1.0F

    private val orientationParams = getOrientationParams(imageBytes)

    private val matrix = Matrix()
    private val bitmapOptions: BitmapFactory.Options

    private val memoryClass: Int

    init {
        quality =
            if (quality == UNSPECIFIED) DEFAULT_MAX_IMAGE_COMPRESSION_QUALITY
            else quality

        resizeAttempts =
            if (resizeAttempts == UNSPECIFIED) DEFAULT_NUMBER_OF_RESIZE_ATTEMPTS
            else resizeAttempts

        // get the image dimensions
        BitmapFactory.Options().apply {
            inJustDecodeBounds = true
            BitmapFactory.decodeStream(
                imageBytes.inputStream(),
                null,
                this
            )
            width = outWidth
            height = outHeight
        }

        bitmapOptions = BitmapFactory.Options().apply {
            inScaled = false
            inDensity = 0
            inTargetDensity = 0
            inSampleSize = 1
            inJustDecodeBounds = false
            inMutable = false
        }

        val am = (context.getSystemService(Context.ACTIVITY_SERVICE) as ActivityManager)
        memoryClass = max(16.0, am.memoryClass.toDouble()).toInt()
    }

    fun resizeGif(): ByteArray? {
        var newWidth = min(width, widthLimit)
        var newHeight = min(height, heightLimit)

        val aspectRatio = (width.toFloat() / height)

        var scaledBytes = imageBytes

        for (attempt in 0 until resizeAttempts) {
            // estimate how much we need to scale the gif down by. if still too big,
            // we'll need to try smaller and smaller values
            val scale = ((byteLimit / scaledBytes.size.toFloat()) * 0.95)

            newWidth = sqrt((scale * newWidth * newHeight) * aspectRatio).toInt()
            newHeight = (newWidth / aspectRatio).toInt()

            val gif = GlideApp
                .with(context)
                .asGif()
                .load(imageBytes)   // always start with original image (for quality)
                .centerInside()
                .encodeQuality(DEFAULT_MAX_IMAGE_COMPRESSION_QUALITY)  // quality here doesn't seem to make much difference
                .submit(newWidth, newHeight)
                .get()

            val outputStream = ByteArrayOutputStream()
            GifEncoder(context, GlideApp.get(context).bitmapPool).encodeTransformedToStream(
                gif,
                outputStream
            )
            scaledBytes = outputStream.toByteArray()

            Timber.d("Gif compression attempt $attempt: ${scaledBytes.size / 1024}/" +
                    "${byteLimit / 1024}Kb ($width*$height -> $newWidth*$newHeight)")

            if (scaledBytes.size <= byteLimit)
                return scaledBytes
        }

        Timber.w("Failed to compress Gif ${imageBytes.size / 1024}Kb to ${byteLimit / 1024}Kb")

        return null
    }

    fun resizeImage(): ByteArray? {
        if (!canBeCompressed())
            return null

        // decode image
        try {
            for (attempts in 0 until resizeAttempts) {
                val scaledBytes = recodeImage(attempts)

                // only return data within the limit
                if ((scaledBytes != null) && (scaledBytes.size <= byteLimit))
                    return scaledBytes

                updateRecodeParameters(scaledBytes?.size ?: 0)
            }
        } catch (e: FileNotFoundException) {
            Timber.e("File disappeared during resizing")
        } finally {
            // release all bitmaps
            if ((scaledBitmap != null) && (scaledBitmap != decodedBitmap))
                scaledBitmap!!.recycle()

            if (decodedBitmap != null)
                decodedBitmap!!.recycle()
        }

        return null
    }

    // choose an initial sub-sample size that ensures the decoded image is no more than
    // MAX_TARGET_SCALE_FACTOR bigger than largest supported image and that it is likely to
    // compress to smaller than the target size (assuming compression down to 1 bit per pixel)
    private fun canBeCompressed(): Boolean {
        var imageHeight = height
        var imageWidth = width

        // assume can use half working memory to decode the initial image (4 bytes per pixel)
        val workingMemoryPixelLimit = (memoryClass * 1024 * 1024 / 8)

        // target 1 bits per pixel in final compressed image
        val finalSizePixelLimit = (byteLimit * 8)

        // when choosing to halve the resolution - only do so the image will still be too big
        // after scaling by MAX_TARGET_SCALE_FACTOR
        val heightLimitWithSlop = (heightLimit * MAX_TARGET_SCALE_FACTOR).toInt()
        val widthLimitWithSlop = (widthLimit * MAX_TARGET_SCALE_FACTOR).toInt()

        val pixelLimitWithSlop = (finalSizePixelLimit *
                MAX_TARGET_SCALE_FACTOR * MAX_TARGET_SCALE_FACTOR).toInt()
        val pixelLimit =
            min(pixelLimitWithSlop.toDouble(), workingMemoryPixelLimit.toDouble()).toInt()

        var sampleSize = 1
        var fits = ((imageHeight < heightLimitWithSlop) &&
                (imageWidth < widthLimitWithSlop) &&
                (imageHeight * imageWidth < pixelLimit))

        // compare sizes to compute sub-sampling needed
        while (!fits) {
            sampleSize *= 2
            // note that recodeImage may try using mSampleSize * 2. Hence we use the factor of 4
            if (sampleSize >= (Int.MAX_VALUE / 4)) {
                Timber.w("Cannot resize image: widthLimit=${widthLimit} " +
                        "heightLimit=${heightLimit} byteLimit=${byteLimit} " +
                        "imageWidth=${width} imageHeight=${height}")
                return false
            }

            Timber.v("computeInitialSampleSize: Increasing sampleSize to $sampleSize " +
                " as h=$imageHeight vs $heightLimitWithSlop w=$imageWidth " +
                " vs $widthLimitWithSlop p=${imageHeight * imageWidth} vs $pixelLimit")

            imageHeight = height / sampleSize
            imageWidth = width / sampleSize

            fits = ((imageHeight < heightLimitWithSlop) &&
                (imageWidth < widthLimitWithSlop) &&
                (imageHeight * imageWidth < pixelLimit))
        }

        Timber.v("computeInitialSampleSize: Initial sampleSize $sampleSize " +
                " as h=$imageHeight vs $heightLimitWithSlop w=$imageWidth " +
                " vs $widthLimitWithSlop p=${imageHeight * imageWidth} vs $pixelLimit")

        this.sampleSize = sampleSize

        return true
    }

    // recode the image from initial uri to encoded JPEG
    @Throws(FileNotFoundException::class)
    private fun recodeImage(attempt: Int): ByteArray? {
        var encoded: ByteArray? = null
        try {
            Timber.v("getResizedImageData: attempt=${attempt} limit (w=$widthLimit" +
                    " h=$heightLimit) quality=$quality scale=$scaleFactor sampleSize=$sampleSize")

            if (scaledBitmap == null) {
                if (decodedBitmap == null) {
                    bitmapOptions.inSampleSize = sampleSize
                    try {
                        imageBytes.inputStream().use {
                            decodedBitmap = BitmapFactory.decodeStream(
                                it,
                                null,
                                bitmapOptions
                            )
                        }
                    } catch (e: IOException) {
                        // ignore
                    }

                    if (decodedBitmap == null) {
                        Timber.v("getResizedImageData: got empty decoded bitmap")
                        return null
                    }
                }

                Timber.v("getResizedImageData: decoded w,h=${decodedBitmap!!.width}," +
                        "${decodedBitmap!!.height}")

                // make sure to scale the decoded image if dimension is not within limit
                val decodedWidth = decodedBitmap!!.width
                val decodedHeight = decodedBitmap!!.height
                if (decodedWidth > widthLimit || decodedHeight > heightLimit) {
                    val minScaleFactor = max(
                        (
                            if (widthLimit == 0) 1.0f
                            else decodedWidth.toFloat() / widthLimit.toFloat()
                        ).toDouble(),
                        (
                            if (heightLimit == 0) 1.0f
                            else decodedHeight.toFloat() / heightLimit.toFloat()
                        ).toDouble()
                    ).toFloat()

                    if (scaleFactor < minScaleFactor)
                        scaleFactor = minScaleFactor
                }

                if (scaleFactor > 1.0 || orientationParams.rotation != 0) {
                    matrix.reset()
                    matrix.postRotate(orientationParams.rotation.toFloat())
                    matrix.postScale(
                        orientationParams.scaleX / scaleFactor,
                        orientationParams.scaleY / scaleFactor
                    )
                    scaledBitmap = Bitmap.createBitmap(
                        decodedBitmap!!, 0, 0, decodedWidth, decodedHeight,
                        matrix, false /* filter */)
                    if (scaledBitmap == null) {
                        Timber.v("getResizedImageData: got empty scaled bitmap")
                        return null
                    }

                    Timber.v("getResizedImageData: scaled w,h=${scaledBitmap!!.width}," +
                            "${scaledBitmap!!.height}")
                } else
                    scaledBitmap = decodedBitmap
            }

            // encode it at current quality
            encoded = bitmapToBytes(scaledBitmap!!, quality)
            if (encoded != null)
                Timber.v("getResizedImageData: Encoded down to ${encoded.size}@" +
                            "${scaledBitmap!!.width}/${scaledBitmap!!.height}q$quality")
        } catch (e: OutOfMemoryError) {
            Timber.w("getResizedImageData - image too big (OutOfMemoryError), " +
                    "will try with smaller scale factor")
            // fall through and keep trying with more compression
        }

        return encoded
    }

    // when image recode fails this method updates compression parameters for the next attempt
    private fun updateRecodeParameters(currentSize: Int) {
        // only return data within the limit
        if ((currentSize > 0) && (quality > MINIMUM_IMAGE_COMPRESSION_QUALITY)) {
            // if everything succeeded but failed to hit target size
            // try quality proportioned to sqrt of size over size limit
            quality = max(
                MINIMUM_IMAGE_COMPRESSION_QUALITY.toDouble(),
                min(
                    (quality * sqrt((1.0 * byteLimit) / currentSize)).toInt().toDouble(),
                    (quality * QUALITY_SCALE_DOWN_RATIO).toInt().toDouble()
                )
            ).toInt()
            Timber.v("getResizedImageData: Retrying at quality $quality")
        } else if ((currentSize > 0) &&
            (scaleFactor < 2.0 * MIN_SCALE_DOWN_RATIO * MIN_SCALE_DOWN_RATIO)
        ) {
            // JPEG compression failed to hit target size - need smaller image
            // first try scaling by a little (< factor of 2) just so long resulting scale down
            // ratio is still significantly bigger than next subsampling step
            // i.e. mScaleFactor/MIN_SCALE_DOWN_RATIO (new scaling factor) <
            //       2.0 / MIN_SCALE_DOWN_RATIO (arbitrary limit)
            quality = DEFAULT_MAX_IMAGE_COMPRESSION_QUALITY
            scaleFactor /= MIN_SCALE_DOWN_RATIO

            Timber.v("getResizedImageData: Retrying at scale $scaleFactor")

            // release scaled bitmap to trigger rescaling
            if ((scaledBitmap != null) && (scaledBitmap != decodedBitmap))
                scaledBitmap!!.recycle()

            scaledBitmap = null
        } else if (currentSize <= 0) {
            // then before we sub-sample try cleaning up our cached memory
            Timber.v("getResizedImageData: Retrying after reclaiming memory ")
        } else {
            // last resort - sub-sample image by another factor of 2 and try again
            sampleSize *= 2
            quality = DEFAULT_MAX_IMAGE_COMPRESSION_QUALITY
            scaleFactor = 1.0f

            Timber.v("getResizedImageData: Retrying at sampleSize $sampleSize")

            // release all bitmaps to trigger subsampling
            if (scaledBitmap != null && scaledBitmap != decodedBitmap)
                scaledBitmap!!.recycle()

            scaledBitmap = null
            if (decodedBitmap != null) {
                decodedBitmap!!.recycle()
                decodedBitmap = null
            }
        }
    }
}

