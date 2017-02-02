package ru.yuksanbo.cxf.transportahc

import io.netty.handler.codec.http.DefaultHttpHeaders
import io.netty.handler.codec.http.HttpHeaders
import org.apache.cxf.Bus
import org.apache.cxf.buslifecycle.BusLifeCycleListener
import org.apache.cxf.common.logging.LogUtils
import org.apache.cxf.configuration.Configurable
import org.apache.cxf.message.Message
import org.apache.cxf.message.MessageImpl
import org.apache.cxf.message.MessageUtils
import org.apache.cxf.phase.PhaseInterceptorChain
import org.apache.cxf.service.model.EndpointInfo
import org.apache.cxf.transport.AbstractConduit
import org.apache.cxf.transport.Conduit
import org.apache.cxf.transport.MessageObserver
import org.apache.cxf.ws.addressing.EndpointReferenceType
import org.asynchttpclient.AsyncHttpClient
import org.asynchttpclient.DefaultAsyncHttpClient
import org.asynchttpclient.RequestBuilder
import org.asynchttpclient.Response
import org.slf4j.LoggerFactory
import ru.yuksanbo.cxf.transportahc.util.Messages
import ru.yuksanbo.cxf.transportahc.util.getAddress
import ru.yuksanbo.cxf.transportahc.util.getContextualBoolean
import ru.yuksanbo.common.timings.Timings
import ru.yuksanbo.common.misc.extract
import java.io.ByteArrayInputStream
import java.io.InputStream
import java.io.OutputStream
import java.util.concurrent.TimeUnit

private val cxfLog by lazy { LogUtils.getL7dLogger(AhcHttpConduit::class.java) }
private val log = LoggerFactory.getLogger(AhcHttpConduit::class.java)
private val emptyByteArray = byteArrayOf()

open class AhcHttpConduit(
        protected val ahcHttpConduitConfig: AhcHttpConduitConfig,
        protected val epInfo: EndpointInfo,
        epReferenceType: EndpointReferenceType?,
        bus: Bus
) : AbstractConduit(getTargetReference(epInfo, epReferenceType, bus)), BusLifeCycleListener, Configurable {

    protected val httpClient: AsyncHttpClient

    init {
        httpClient = DefaultAsyncHttpClient(ahcHttpConduitConfig.httpClientConfig)
    }

    override fun prepare(message: Message) {
        val timings = Timings("Cxf message processing", ahcHttpConduitConfig.timingsThreshold)

        timings.takeReading("Preparing cxf message for sending")

        val address = message.getAddress()

        var httpRequestMethod = message[Message.HTTP_REQUEST_METHOD] as? String
        if (httpRequestMethod == null) {
            httpRequestMethod = "POST"
            message.put(Message.HTTP_REQUEST_METHOD, "POST")
        }

        val requestHeaders = DefaultHttpHeaders()

        val requestBuilder = RequestBuilder(httpRequestMethod)
                .setUrl(address)
                .setRequestTimeout(Messages.getRequestTimeout(message, ahcHttpConduitConfig.httpClientConfig.requestTimeout))
                .setCharset(Charsets.UTF_8)

        message.getContextualBoolean(Properties.FollowRedirect)?.let { requestBuilder.setFollowRedirect(it) }

        requestHeaders.add("Content-Type", message[Message.CONTENT_TYPE]!!.toString())

        //todo: set realm properties (basic, etc)

        val requestContext = AhcRequestContext(address, requestHeaders, requestBuilder, timings)

        message.setContent(
                OutputStream::class.java,
                AhcOutputStream(
                        ahcHttpConduitConfig.bufferPerRequestSize,
                        requestContext,
                        { r -> sendMessage(message, r) }
                )
        )

        timings.takeReading("Created cxf OutputStream and prepared request")
    }

    override fun getLogger() = cxfLog!!

    override fun initComplete() {

    }

    override fun preShutdown() {

    }

    override fun postShutdown() {
        try {
            log.debug("Shutting down AsyncHttpClient")
            httpClient.close()
            log.debug("AsyncHttpClient shutdown complete")
        }
        catch (t: Throwable) {
            log.error("Failed to close AsyncHttpClient", t)
        }
    }

    override fun getBeanName(): String? {
        if (epInfo.name != null) {
            return epInfo.name.toString() + ".http-conduit"
        }
        return null
    }

    @SuppressWarnings("unchecked")
    private fun enrichWithProtocolHeaders(message: Message, requestHeaders: HttpHeaders) {
        message[Message.PROTOCOL_HEADERS]?.let { messageHeaders ->
            val headersMap = try {
                messageHeaders as Map<String, MutableList<String>>
            }
            catch (e: ClassCastException) {
                throw IllegalArgumentException("Can't cast message headers to Map<String, List<String>>", e)
            }

            headersMap.forEach { name, value -> requestHeaders.add(name, value) }
        }
    }

    internal fun sendMessage(message: Message, requestContext: AhcRequestContext) {
        enrichWithProtocolHeaders(message, requestContext.headers)

        requestContext.ahcRequestBuilder.setHeaders(requestContext.headers)

        val timings = requestContext.timings

        timings.takeReading("Before building request")
        requestContext.request = requestContext.ahcRequestBuilder.build()
        timings.takeReading("After building request")

        val responseFuture = httpClient.executeRequest(requestContext.request).toCompletableFuture()
        timings.takeReading("After executing request")

        val currentRequestTimeoutMillis = requestContext.request!!.requestTimeout.toLong()

        if (MessageUtils.getContextualBoolean(message, Properties.Async, false)) {

            timings.takeReading("Waiting for response asynchronously")

            responseFuture.whenComplete { r, t ->
                try {
                    if (t != null) {
                        timings.takeReading("Got backend exception from ahc")
                        passToObserver(message, t)
                        timings.takeReading("Fault processed")
                        return@whenComplete
                    }

                    timings.takeReading("Got response from ahc")

                    try {
                        passToObserver(r, message, requestContext)
                    }
                    catch (t2: Throwable) {
                        timings.takeReading("Exception occurred while processing input message")
                        passToObserver(message, t2)
                        timings.takeReading("Fault processed")
                    }
                }
                finally {
                    timings.report()
                }

            }
        }
        else {
            //sync
            try {
                timings.takeReading("Waiting for response synchronously")
                val ahcResponse = responseFuture.get(currentRequestTimeoutMillis, TimeUnit.MILLISECONDS)
                timings.takeReading("Got response from ahc")
                passToObserver(ahcResponse, message, requestContext)
            }
            catch (t: Throwable) {
                timings.takeReading("Exception occurred while processing response from ahc")
                try {
                    passToObserver(message, t)
                }
                catch (t2: Throwable) {
                    log.error("Failed to pass fault message to observer", t2)
                }
                timings.takeReading("Fault processed")
            }
            finally {
                timings.report()
            }

        }

    }

    /**
     * Sending input message to incoming observer
     */
    open fun passToObserver(ahcResponse: Response, requestMessage: Message, requestContext: AhcRequestContext) {
        formResponseMessage(requestMessage, ahcResponse, requestContext)?.let { incomingObserver.onMessage(it) }
    }

    /**
     * Sending fault to corresponding observer
     */
    open fun passToObserver(requestMessage: Message, t: Throwable) {
        log.error("Exception occurred in async http client, generating fault", t)
        val phaseInterceptorChain = requestMessage.interceptorChain as PhaseInterceptorChain

        // stop all interceptors
        phaseInterceptorChain.abort()
        requestMessage.setContent(Exception::class.java, t)

        // call interceptors (reverse)
        phaseInterceptorChain.unwind(requestMessage)

        //find observer
        val mo: MessageObserver = (phaseInterceptorChain.faultObserver ?: requestMessage.exchange.get(MessageObserver::class.java))
                ?: throw IllegalStateException("Couldn't find fault message observer. Fault cause: " + t.message)

        mo.onMessage(requestMessage)
    }

    // copy-paste from httpconduit
    open fun handleResponseCode(requestMessage: Message, ahcResponse: Response, requestContext: AhcRequestContext): Int {
        val exchange = requestMessage.exchange!!
        val responseCode = ahcResponse.statusCode

        if (responseCode == -1) {
            log.warn("HTTP Response code appears to be corrupted")
        }

        exchange.put(Message.RESPONSE_CODE, responseCode)

        // "org.apache.cxf.transport.no_io_exceptions" property should be set in case the error response code
        // should not be handled here; for example jax rs uses this

        // "org.apache.cxf.transport.process_fault_on_http_400" property should be set in case a
        // soap fault because of a HTTP 400 should be returned back to the client (SOAP 1.2 spec)

        val is4xx5xx = responseCode >= 400
        val not500 = responseCode != 500
        val ioExceptions = !MessageUtils.isTrue(requestMessage.getContextualProperty("org.apache.cxf.transport.no_io_exceptions"))
        val noFaultOn400 = !MessageUtils.isTrue(requestMessage.getContextualProperty("org.apache.cxf.transport.process_fault_on_http_400"))
        val not400 = responseCode != 400

        if (is4xx5xx && not500 && ioExceptions && (not400 || noFaultOn400)) {

            if (responseCode == 400 || responseCode == 503) {
                exchange.put("org.apache.cxf.transport.service_not_available", true)
            }

            throw HttpException(responseCode, ahcResponse.statusText, requestContext.address)
        }

        return responseCode
    }

    private fun parseCharset(contentType: String): String? {
        val charset = contentType.extract("charset=")?.let { it.filter { it != '\"' && it != '\'' }}
        return charset
    }

    private fun checkNotOneWay(message: Message, responseCode: Int): Boolean {
        // 1. Not oneWay
        val exchange = message.exchange
        if (exchange == null || !exchange.isOneWay) {
            return true
        }
        // 2. Robust OneWays could have a fault
        return responseCode == 500 && MessageUtils.getContextualBoolean(message, Message.ROBUST_ONEWAY, false)
    }

    @Throws(HttpException::class)
    open fun formResponseMessage(requestMessage: Message, ahcResponse: Response, ahcRequestContext: AhcRequestContext): Message? {
        val exchange = requestMessage.exchange
        val responseCode = handleResponseCode(requestMessage, ahcResponse, ahcRequestContext)

        val responseMessage = MessageImpl()

        responseMessage.exchange = exchange

        val contentType = ahcResponse.contentType
        responseMessage.put(Message.CONTENT_TYPE, contentType)

        responseMessage.put(Message.RESPONSE_CODE, responseCode)

        responseMessage.put(Conduit::class.java, this)

        //oneway or http-accepted (202)
        if (!checkNotOneWay(requestMessage, responseCode) || responseCode == 202) {
            throw UnsupportedOperationException("Oneway message processing not implemented yet")
        } else {
            //not going to be resending or anything, clear stream
            requestMessage.removeContent(OutputStream::class.java)
        }

        contentType?.let {
            parseCharset(it)?.let { charset -> responseMessage.put(Message.ENCODING, charset) }
        }

        //todo: check who closes this stream
        val inputStream = ahcResponse.responseBodyAsStream ?: ByteArrayInputStream(emptyByteArray)

        responseMessage.setContent(InputStream::class.java, inputStream)

        return responseMessage
    }

    object Properties {
        private val prefix = AhcHttpConduit::class.java.name + "."

        /* HTTP related */
        @JvmField
        val FollowRedirect = prefix + "follow-redirect"

        /* Conduit specific contextual properties */
        @JvmField
        val RequestTimeout = prefix + "request-timeout"

        /* CXF specific */
        @JvmField
        val Async = "use.async.http.conduit"
    }

}