package com.ahmedmahmoud.cinemonitor.camera

import android.graphics.Bitmap
import android.graphics.BitmapFactory
import android.hardware.usb.UsbConstants
import android.hardware.usb.UsbDevice
import android.hardware.usb.UsbDeviceConnection
import android.hardware.usb.UsbEndpoint
import android.hardware.usb.UsbInterface
import android.hardware.usb.UsbManager
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.util.concurrent.ArrayBlockingQueue

class CanonPtpController(private val usbManager: UsbManager) {

    companion object {
        const val CANON_VENDOR_ID = 0x04A9

        const val PTP_OC_OPEN_SESSION: Short = 0x1002
        const val PTP_OC_CANON_SET_DEVICE_PROP_EX: Short = 0x9110.toShort()
        const val PTP_OC_CANON_SET_REMOTE_MODE: Short = 0x9114.toShort()
        const val PTP_OC_CANON_SET_EVENT_MODE: Short = 0x9115.toShort()
        const val PTP_OC_CANON_GET_EVENT_DATA: Short = 0x9116.toShort()
        const val PTP_OC_CANON_REMOTE_RELEASE_ON: Short = 0x9128.toShort()
        const val PTP_OC_CANON_REMOTE_RELEASE_OFF: Short = 0x9129.toShort()
        const val PTP_OC_CANON_GET_VIEWFINDER_DATA: Short = 0x9153.toShort()
        const val PTP_OC_CANON_SET_LV_ZOOM_POS: Short = 0x9158.toShort()
        const val PTP_OC_CANON_DO_AF: Short = 0x9160.toShort()

        const val PROP_APERTURE = 0xD101
        const val PROP_SHUTTER_SPEED = 0xD102
        const val PROP_ISO = 0xD103
        const val PROP_WHITE_BALANCE = 0xD109
        const val PROP_COLOR_TEMP = 0xD10A
        const val PROP_EVF_OUTPUT_DEVICE = 0xD1B0
        const val PROP_EVF_MODE = 0xD1B1
        const val PROP_RECORDING_STATE = 0xD1B8
        const val WB_MODE_COLOR_TEMP = 9

        private const val BITMAP_POOL_CAPACITY = 3
        private const val USB_CHUNK_SIZE = 16384
        private const val MAX_CONSECUTIVE_USB_FAILURES = 25
        private const val FIRST_FRAME_TIMEOUT_NS = 30_000_000_000L
        private const val FRAME_STALL_TIMEOUT_NS = 2_500_000_000L
        private val EVF_REQUEST_PAYLOAD = byteArrayOf(0x00, 0x00, 0x10, 0x00)
    }

    private var currentDevice: UsbDevice? = null
    @Volatile private var connection: UsbDeviceConnection? = null
    private var usbInterface: UsbInterface? = null
    private var epOut: UsbEndpoint? = null
    private var epIn: UsbEndpoint? = null
    private var transactionId = 1
    @Volatile private var streamingJob: Job? = null
    private var sessionInitialized = false
    @Volatile var lastDiagnosticError: String = ""
        private set
    private val usbLock = Any()

    // Reusable USB I/O transfer buffer (always accessed under usbLock)
    private val usbChunkBuffer = ByteArray(USB_CHUNK_SIZE)

    // Triple-buffer Bitmap pool for zero-allocation inBitmap decoding
    private val bitmapPool = ArrayBlockingQueue<Bitmap>(BITMAP_POOL_CAPACITY)

    private val _cameraState = MutableStateFlow(CameraLiveState())
    val cameraState: StateFlow<CameraLiveState> = _cameraState

    @Volatile
    var onEvfFrameDecoded: ((Bitmap) -> Unit)? = null

    fun connect(device: UsbDevice): Boolean {
        if (device.vendorId != CANON_VENDOR_ID) return false

        disconnect()

        var foundInterface: UsbInterface? = null
        var foundOut: UsbEndpoint? = null
        var foundIn: UsbEndpoint? = null

        for (i in 0 until device.interfaceCount) {
            val intf = device.getInterface(i)
            if (intf.interfaceClass == UsbConstants.USB_CLASS_STILL_IMAGE) {
                for (j in 0 until intf.endpointCount) {
                    val ep = intf.getEndpoint(j)
                    if (ep.type == UsbConstants.USB_ENDPOINT_XFER_BULK) {
                        if (ep.direction == UsbConstants.USB_DIR_OUT) foundOut = ep
                        if (ep.direction == UsbConstants.USB_DIR_IN) foundIn = ep
                    }
                }
                if (foundOut != null && foundIn != null) {
                    foundInterface = intf
                    break
                }
            }
        }

        lastDiagnosticError = ""
        val intf = foundInterface ?: run {
            lastDiagnosticError = "No PTP interface/endpoints found on camera"
            return false
        }
        val conn = usbManager.openDevice(device) ?: run {
            lastDiagnosticError = "usbManager.openDevice() returned null"
            return false
        }

        if (!conn.claimInterface(intf, true)) {
            runCatching { conn.close() }
            lastDiagnosticError = "claimInterface() failed (close Camera Connect / check USB mode)"
            return false
        }

        usbInterface = intf
        epOut = foundOut
        epIn = foundIn
        currentDevice = device
        connection = conn
        transactionId = 1
        sessionInitialized = false

        val initialized = runCatching { initializeRemoteLiveViewSession() }.getOrElse { err ->
            lastDiagnosticError = "Init exception: ${err.message}"
            false
        }
        if (!initialized) {
            closeConnectionResources()
            return false
        }
        sessionInitialized = true
        return true
    }

    private fun initializeRemoteLiveViewSession(): Boolean {
        if (!sendCommand(PTP_OC_OPEN_SESSION, intArrayOf(1))) {
            lastDiagnosticError = "PTP OpenSession (0x1002) failed"
            return false
        }
        if (!sendCommand(PTP_OC_CANON_SET_REMOTE_MODE, intArrayOf(1))) {
            lastDiagnosticError = "SetRemoteMode (0x9114) failed - check Camera USB App setting"
            return false
        }
        if (!sendCommand(PTP_OC_CANON_SET_EVENT_MODE, intArrayOf(1))) {
            lastDiagnosticError = "SetEventMode (0x9115) failed"
            return false
        }
        // Gradual EVF activation: do not abort session if R-series rejects one output value
        setCameraPropertyChecked(PROP_EVF_MODE, 1)
        if (!setCameraPropertyChecked(PROP_EVF_OUTPUT_DEVICE, 3)) {
            setCameraPropertyChecked(PROP_EVF_OUTPUT_DEVICE, 2)
        }
        return true
    }

    fun startDirectUsbLiveViewAndEvents(
        scope: CoroutineScope,
        onStreamStateChanged: (Boolean) -> Unit,
        onConnectionLost: () -> Unit = {}
    ) {
        streamingJob?.cancel()
        streamingJob = scope.launch(Dispatchers.IO) {
            var loopCounter = 0
            var frameCount = 0
            var consecutiveFailures = 0
            var lastFpsTimestampNs = android.os.SystemClock.elapsedRealtimeNanos()
            val streamStartNs = lastFpsTimestampNs
            var lastFrameTimestampNs = 0L
            var isStreamingActive = false
            var connectionLostNotified = false

            val decodeOptions = BitmapFactory.Options().apply {
                inPreferredConfig = Bitmap.Config.ARGB_8888
                inMutable = true
                inSampleSize = 1
            }

            try {
                while (isActive && connection != null) {
                    try {
                        if (loopCounter % 5 == 0) {
                            val eventData = synchronized(usbLock) {
                                if (connection == null) return@synchronized null
                                val txId = transactionId++
                                val cmdPacket = buildPtpPacket(type = 1, code = PTP_OC_CANON_GET_EVENT_DATA, txId = txId)
                                if (safeBulkOut(epOut, cmdPacket, 500) > 0) {
                                    readFullPtpDataResponse(txId)
                                } else {
                                    null
                                }
                            }
                            if (eventData != null) {
                                _cameraState.value = CanonEventParser.parseEventPacket(
                                    rawBytes = eventData,
                                    currentState = _cameraState.value
                                )
                            }
                        }
                        loopCounter++

                        var outTransferOk = false
                        val evfPayload = synchronized(usbLock) {
                            if (connection == null) return@synchronized null
                            val txId = transactionId++
                            val cmdPacket = buildPtpPacket(
                                type = 1,
                                code = PTP_OC_CANON_GET_VIEWFINDER_DATA,
                                txId = txId,
                                payload = EVF_REQUEST_PAYLOAD
                            )
                            val sent = safeBulkOut(epOut, cmdPacket, 500)
                            outTransferOk = sent > 0
                            if (outTransferOk) readFullPtpDataResponse(txId) else null
                        }

                        if (!outTransferOk) {
                            consecutiveFailures++
                            if (consecutiveFailures >= MAX_CONSECUTIVE_USB_FAILURES) {
                                withContext(Dispatchers.Main) { onConnectionLost() }
                                break
                            }
                            delay(25)
                            continue
                        }

                        consecutiveFailures = 0

                        if (evfPayload != null && evfPayload.size > 64) {
                            val jpegBitmap = extractAndDecodeJpegPooled(evfPayload, decodeOptions)
                            if (jpegBitmap != null) {
                                if (!isStreamingActive) {
                                    isStreamingActive = true
                                    withContext(Dispatchers.Main) { onStreamStateChanged(true) }
                                }
                                val frameCallback = onEvfFrameDecoded
                                if (frameCallback != null) {
                                    frameCallback.invoke(jpegBitmap)
                                } else {
                                    releaseEvfBitmap(jpegBitmap)
                                }

                                val nowNs = android.os.SystemClock.elapsedRealtimeNanos()
                                lastFrameTimestampNs = nowNs
                                frameCount++
                                if (nowNs - lastFpsTimestampNs >= 1_000_000_000L) {
                                    _cameraState.value = _cameraState.value.copy(liveViewFps = frameCount)
                                    frameCount = 0
                                    lastFpsTimestampNs = nowNs
                                }
                            }
                        } else {
                            if (loopCounter % 20 == 0) {
                                setCameraProperty(PROP_EVF_MODE, 1)
                                if (!setCameraPropertyChecked(PROP_EVF_OUTPUT_DEVICE, 3)) {
                                    setCameraPropertyChecked(PROP_EVF_OUTPUT_DEVICE, 2)
                                }
                            }
                            delay(15)
                        }

                        val watchdogNowNs = android.os.SystemClock.elapsedRealtimeNanos()
                        val stalled = if (lastFrameTimestampNs == 0L) {
                            watchdogNowNs - streamStartNs > FIRST_FRAME_TIMEOUT_NS
                        } else {
                            watchdogNowNs - lastFrameTimestampNs > FRAME_STALL_TIMEOUT_NS
                        }
                        if (stalled && !connectionLostNotified) {
                            connectionLostNotified = true
                            withContext(Dispatchers.Main) { onConnectionLost() }
                            break
                        }
                    } catch (_: CancellationException) {
                        break
                    } catch (_: Exception) {
                        consecutiveFailures++
                        if (consecutiveFailures >= MAX_CONSECUTIVE_USB_FAILURES) {
                            withContext(Dispatchers.Main) { onConnectionLost() }
                            break
                        }
                        delay(40)
                    }
                }
            } finally {
                if (isStreamingActive) {
                    withContext(NonCancellable + Dispatchers.Main.immediate) {
                        onStreamStateChanged(false)
                    }
                }
                clearBitmapPool()
            }
        }
    }

    fun releaseEvfBitmap(bitmap: Bitmap) {
        if (bitmap.isRecycled || !bitmap.isMutable) return
        if (connection == null || streamingJob?.isActive != true) {
            bitmap.recycle()
            return
        }
        if (!bitmapPool.offer(bitmap)) {
            bitmap.recycle()
        }
    }

    private fun clearBitmapPool() {
        while (true) {
            val bmp = bitmapPool.poll() ?: break
            if (!bmp.isRecycled) {
                bmp.recycle()
            }
        }
    }

    private fun extractAndDecodeJpegPooled(
        ptpDataBlock: ByteArray,
        options: BitmapFactory.Options
    ): Bitmap? {
        val searchLimit = minOf(ptpDataBlock.size - 3, 512)
        var jpegStart = -1
        for (i in 12 until searchLimit) {
            if (ptpDataBlock[i] == 0xFF.toByte() &&
                ptpDataBlock[i + 1] == 0xD8.toByte() &&
                ptpDataBlock[i + 2] == 0xFF.toByte()
            ) {
                jpegStart = i
                break
            }
        }
        if (jpegStart < 0) return null

        val jpegLength = ptpDataBlock.size - jpegStart
        if (jpegLength <= 100) return null

        var candidate: Bitmap? = null
        while (true) {
            val polled = bitmapPool.poll() ?: break
            if (!polled.isRecycled && polled.isMutable) {
                candidate = polled
                break
            }
        }

        options.inSampleSize = 1
        options.inMutable = true
        options.inPreferredConfig = Bitmap.Config.ARGB_8888
        options.inBitmap = candidate

        return try {
            val decoded = BitmapFactory.decodeByteArray(ptpDataBlock, jpegStart, jpegLength, options)
            if (decoded == null && candidate != null) {
                releaseEvfBitmap(candidate)
            }
            decoded
        } catch (_: IllegalArgumentException) {
            candidate?.recycle()
            options.inBitmap = null
            try {
                BitmapFactory.decodeByteArray(ptpDataBlock, jpegStart, jpegLength, options)
            } catch (_: Exception) {
                null
            }
        } catch (_: Exception) {
            if (candidate != null) releaseEvfBitmap(candidate)
            null
        }
    }

    fun setCameraProperty(propCode: Int, canonValueCode: Int) {
        setCameraPropertyChecked(propCode, canonValueCode)
    }

    private fun setCameraPropertyChecked(propCode: Int, canonValueCode: Int): Boolean =
        synchronized(usbLock) {
            setCameraPropertyLocked(propCode, canonValueCode)
        }

    private fun setCameraPropertyLocked(propCode: Int, canonValueCode: Int): Boolean {
        if (connection == null) return false
        val txId = transactionId++
        val cmdPacket = buildPtpPacket(type = 1, code = PTP_OC_CANON_SET_DEVICE_PROP_EX, txId = txId)
        if (safeBulkOut(epOut, cmdPacket, 600) != cmdPacket.size) return false

        val payload = ByteBuffer.allocate(12).order(ByteOrder.LITTLE_ENDIAN).apply {
            putInt(12)
            putInt(propCode)
            putInt(canonValueCode)
        }.array()

        val dataPacket = buildPtpPacket(type = 2, code = PTP_OC_CANON_SET_DEVICE_PROP_EX, txId = txId, payload = payload)
        if (safeBulkOut(epOut, dataPacket, 600) != dataPacket.size) return false
        return readSimpleResponse(txId)
    }

    fun setWhiteBalanceKelvin(kelvin: Int) {
        val clamped = kelvin.coerceIn(2500, 10000)
        synchronized(usbLock) {
            setCameraPropertyLocked(PROP_WHITE_BALANCE, WB_MODE_COLOR_TEMP)
            setCameraPropertyLocked(PROP_COLOR_TEMP, clamped)
        }
        _cameraState.value = _cameraState.value.copy(whiteBalanceKelvin = clamped)
    }

    fun toggleVideoRecording(start: Boolean) {
        val mode = if (start) 4 else 0
        setCameraProperty(PROP_RECORDING_STATE, mode)
    }

    fun triggerTouchAf(canonX: Int, canonY: Int, scope: CoroutineScope) {
        scope.launch(Dispatchers.IO) {
            sendCommand(PTP_OC_CANON_SET_LV_ZOOM_POS, intArrayOf(canonX, canonY))
            sendCommand(PTP_OC_CANON_DO_AF, intArrayOf())
            if (!_cameraState.value.isRecording) {
                sendCommand(PTP_OC_CANON_REMOTE_RELEASE_ON, intArrayOf(1, 0))
                delay(120)
                sendCommand(PTP_OC_CANON_REMOTE_RELEASE_OFF, intArrayOf(1, 0))
            }
        }
    }

    fun isMatchingDevice(device: UsbDevice?): Boolean {
        if (device == null) return true
        val active = currentDevice ?: return device.vendorId == CANON_VENDOR_ID
        return device.deviceId == active.deviceId
    }

    private fun safeBulkOut(endpoint: UsbEndpoint?, data: ByteArray, timeoutMs: Int): Int {
        val conn = connection ?: return -1
        val ep = endpoint ?: return -1
        return try {
            conn.bulkTransfer(ep, data, data.size, timeoutMs).let { transferred ->
                if (transferred == data.size) transferred else -1
            }
        } catch (_: Exception) {
            -1
        }
    }

    private fun safeBulkIn(endpoint: UsbEndpoint?, buffer: ByteArray, timeoutMs: Int): Int {
        val conn = connection ?: return -1
        val ep = endpoint ?: return -1
        return try {
            conn.bulkTransfer(ep, buffer, buffer.size, timeoutMs)
        } catch (_: Exception) {
            -1
        }
    }

    private fun sendCommand(opcode: Short, params: IntArray): Boolean {
        return synchronized(usbLock) {
            if (connection == null) return@synchronized false
            val txId = transactionId++
            val payload = ByteBuffer.allocate(params.size * 4).order(ByteOrder.LITTLE_ENDIAN).apply {
                params.forEach { putInt(it) }
            }.array()
            val packet = buildPtpPacket(type = 1, code = opcode, txId = txId, payload = payload)
            if (safeBulkOut(epOut, packet, 600) != packet.size) return@synchronized false
            readSimpleResponse(txId)
        }
    }

    private fun buildPtpPacket(type: Short, code: Short, txId: Int, payload: ByteArray = ByteArray(0)): ByteArray {
        val totalLength = 12 + payload.size
        return ByteBuffer.allocate(totalLength).order(ByteOrder.LITTLE_ENDIAN).apply {
            putInt(totalLength)
            putShort(type)
            putShort(code)
            putInt(txId)
            put(payload)
        }.array()
    }

    private fun readSimpleResponse(expectedTransactionId: Int): Boolean {
        val bytesRead = safeBulkIn(epIn, usbChunkBuffer, 600)
        if (bytesRead < 12) return false

        val buffer = ByteBuffer.wrap(usbChunkBuffer, 0, bytesRead).order(ByteOrder.LITTLE_ENDIAN)
        val containerLength = buffer.int
        val containerType = buffer.short.toInt() and 0xFFFF
        val responseCode = buffer.short.toInt() and 0xFFFF
        val responseTransactionId = buffer.int

        return containerLength >= 12 &&
            containerLength <= bytesRead &&
            containerType == 0x0003 &&
            responseCode == 0x2001 &&
            responseTransactionId == expectedTransactionId
    }

    private fun readFullPtpDataResponse(expectedTransactionId: Int): ByteArray? {
        val firstRead = safeBulkIn(epIn, usbChunkBuffer, 600)
        if (firstRead < 12) return null

        val header = ByteBuffer.wrap(usbChunkBuffer, 0, 12).order(ByteOrder.LITTLE_ENDIAN)
        val totalDataSize = header.int
        val containerType = header.short.toInt() and 0xFFFF
        header.short // operation code
        val responseTransactionId = header.int
        if (containerType != 0x0002 || responseTransactionId != expectedTransactionId) return null

        if (totalDataSize <= 12 || totalDataSize > 1_500_000) return null

        val fullBuffer = ByteArray(totalDataSize)
        var copied = minOf(firstRead, totalDataSize)
        System.arraycopy(usbChunkBuffer, 0, fullBuffer, 0, copied)

        var trailingBytesAlreadyRead = (firstRead - totalDataSize).coerceAtLeast(0)

        while (copied < totalDataSize) {
            val n = safeBulkIn(epIn, usbChunkBuffer, 600)
            if (n <= 0) break
            val toCopy = minOf(n, totalDataSize - copied)
            System.arraycopy(usbChunkBuffer, 0, fullBuffer, copied, toCopy)
            copied += toCopy
            trailingBytesAlreadyRead = (n - toCopy).coerceAtLeast(0)
        }

        if (trailingBytesAlreadyRead < 12) {
            safeBulkIn(epIn, usbChunkBuffer, 120)
        }

        return if (copied == totalDataSize) fullBuffer else null
    }

    fun disconnect() {
        streamingJob?.cancel()
        streamingJob = null
        synchronized(usbLock) {
            val activeConn = connection
            val activeIntf = usbInterface
            val activeOut = epOut
            val activeIn = epIn
            if (activeConn != null) {
                if (sessionInitialized) {
                    runCatching {
                        val txId = transactionId++
                        val cmdPacket = buildPtpPacket(type = 1, code = PTP_OC_CANON_SET_DEVICE_PROP_EX, txId = txId)
                        if (activeConn.bulkTransfer(activeOut, cmdPacket, cmdPacket.size, 100) == cmdPacket.size) {
                            val payload = ByteBuffer.allocate(12).order(ByteOrder.LITTLE_ENDIAN).apply {
                                putInt(12)
                                putInt(PROP_EVF_OUTPUT_DEVICE)
                                putInt(1)
                            }.array()
                            val dataPacket = buildPtpPacket(type = 2, code = PTP_OC_CANON_SET_DEVICE_PROP_EX, txId = txId, payload = payload)
                            if (activeConn.bulkTransfer(activeOut, dataPacket, dataPacket.size, 100) == dataPacket.size) {
                                val ack = ByteArray(64)
                                activeConn.bulkTransfer(activeIn, ack, ack.size, 100)
                            }
                        }
                    }
                }
                if (activeIntf != null) runCatching { activeConn.releaseInterface(activeIntf) }
                runCatching { activeConn.close() }
            }
            connection = null
            usbInterface = null
            epOut = null
            epIn = null
            currentDevice = null
            sessionInitialized = false
            clearBitmapPool()
            _cameraState.value = CameraLiveState()
        }
    }

    private fun closeConnectionResources() {
        synchronized(usbLock) {
            val activeConn = connection
            val activeIntf = usbInterface
            if (activeConn != null) {
                if (activeIntf != null) runCatching { activeConn.releaseInterface(activeIntf) }
                runCatching { activeConn.close() }
            }
            connection = null
            usbInterface = null
            epOut = null
            epIn = null
            currentDevice = null
            sessionInitialized = false
            clearBitmapPool()
            _cameraState.value = CameraLiveState()
        }
    }
}
