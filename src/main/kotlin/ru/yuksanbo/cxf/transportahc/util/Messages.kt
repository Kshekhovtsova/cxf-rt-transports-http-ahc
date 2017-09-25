package ru.yuksanbo.cxf.transportahc.util

import org.apache.cxf.common.util.PropertyUtils
import org.apache.cxf.message.Message
import org.asynchttpclient.Realm
import ru.yuksanbo.cxf.transportahc.AhcHttpConduit
import java.net.URISyntaxException

inline fun Message.getContextualBoolean(key: String): Boolean? {
    val property = this.getContextualProperty(key)
    property?.let {
        return PropertyUtils.isTrue(property)
    }

    return null
}

@Throws(URISyntaxException::class)
inline fun Message.getAddress(): String {
    val endpointAddress = this[Message.ENDPOINT_ADDRESS] as? String
    var result = endpointAddress!!
    val pathInfo = this[Message.PATH_INFO] as? String
    val queryString = this[Message.QUERY_STRING] as? String

    pathInfo?.let {
        if (!result.endsWith(it)) {
            result += pathInfo
        }
    }

    queryString?.let {
        result += "?" + queryString
    }

    return result
}

object Messages {
    //not extension function, cause this method is useful only for this transport
    fun getRequestTimeout(message: Message, default: Int): Int {
        var requestTimeout = default

        val msgRequestTimeout = message.getContextualProperty(AhcHttpConduit.Properties.RequestTimeout) as? String
        if (msgRequestTimeout != null) {
            requestTimeout = msgRequestTimeout.toInt()
        }

        return requestTimeout
    }

    fun getRealm(message: Message): Realm? =  message.getContextualProperty(AhcHttpConduit.Properties.Realm) as? Realm

}