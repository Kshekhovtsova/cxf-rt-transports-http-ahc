package ru.yuksanbo.cxf.transportahc

import com.typesafe.config.Config
import org.apache.cxf.Bus
import org.apache.cxf.configuration.Configurer
import org.apache.cxf.service.model.EndpointInfo
import org.apache.cxf.transport.AbstractTransportFactory
import org.apache.cxf.transport.Conduit
import org.apache.cxf.transport.ConduitInitiator
import org.apache.cxf.transport.http.HTTPTransportFactory
import org.apache.cxf.ws.addressing.EndpointReferenceType
import com.typesafe.config.ConfigSyntax
import com.typesafe.config.ConfigParseOptions
import com.typesafe.config.ConfigFactory
import io.netty.channel.EventLoopGroup
import io.netty.handler.ssl.SslContext
import io.netty.util.Timer
import org.slf4j.LoggerFactory

val ahcPrefix = "ahc://"
val supportedUriPrefixes = mutableSetOf("http://", "https://", ahcPrefix)

open class AhcTransportFactory : AbstractTransportFactory, ConduitInitiator {

    companion object {
        @JvmField
        val DEFAULT_NAMESPACES = mutableListOf("urn:X-cxf-conduit:ahc")

        private val log = LoggerFactory.getLogger(AhcTransportFactory::class.java)
        private val conduitDefaults: Config?

        init {
            DEFAULT_NAMESPACES.addAll(HTTPTransportFactory.DEFAULT_NAMESPACES)

            conduitDefaults = try {
                loadDefaults()
            }
            catch (t: Throwable) {
                log.error("Failed to load AhcHttpConduit defaults", t)
                null
            }
        }

        private fun loadDefaults(): Config {
            return ConfigFactory.parseResourcesAnySyntax(
                    AhcTransportFactory::class.java,
                    "/ru/yuksanbo/cxf/transportahc/ahc-defaults.conf",
                    ConfigParseOptions.defaults().setSyntax(ConfigSyntax.CONF)
            )
        }
    }

    val conduitConfig: AhcHttpConduitConfig

    constructor() : super(DEFAULT_NAMESPACES) {
        conduitConfig = handleExceptions { AhcHttpConduitConfig.from(conduitDefaults!!) }
    }

    @JvmOverloads
    constructor(
            config: Config,
            sslContext: SslContext? = null,
            threadPool: EventLoopGroup? = null,
            timer: Timer? = null
    ) : super(DEFAULT_NAMESPACES) {
        conduitConfig = handleExceptions {
            AhcHttpConduitConfig.from(
                    config.withFallback(conduitDefaults!!).resolve(),
                    sslContext,
                    threadPool,
                    timer
            )
        }
    }

    //todo:
//    @JvmOverloads
//    private constructor(
//            map: MutableMap<String?, String?>,
//            sslContext: SslContext? = null,
//            threadPool: EventLoopGroup? = null,
//            timer: Timer? = null
//    ) : super(DEFAULT_NAMESPACES) {
//        conduitConfig = handleExceptions { AhcHttpConduitConfig.from(map, conduitDefaults!!, sslContext, threadPool, timer) }
//    }

    private fun handleExceptions(init: () -> AhcHttpConduitConfig): AhcHttpConduitConfig {
        try {
            return init.invoke()
        } catch (t: Throwable) {
            //cxf not logging it
            log.error("Failed to initialize Ahc conduit config", t)
            throw t
        }
    }

    override fun getUriPrefixes(): MutableSet<String> = supportedUriPrefixes

    override fun getConduit(targetInfo: EndpointInfo, bus: Bus) = getConduit(targetInfo, targetInfo.target, bus)

    override fun getConduit(localInfo: EndpointInfo, target: EndpointReferenceType, bus: Bus): Conduit {
        val conduit = AhcHttpConduit(
                conduitConfig,
                localInfo,
                target,
                bus
        )

        val address = getAddress(target)
        localInfo.address = address

        configure(bus, conduit, conduit.beanName, address)
        return conduit
    }

    protected fun configure(bus: Bus, bean: Any, name: String?, extraName: String?) {
        val beanConfigurer = bus.getExtension(Configurer::class.java)
        beanConfigurer?.let { bc ->
            bc.configureBean(name, bean)
            extraName?.let {
                bc.configureBean(it, bean)
            }
        }
    }

    protected fun getAddress(target: EndpointReferenceType): String {
        var address = target.address.value!!

        if (address.startsWith("ahc://")) {
            address = address.substring(6)
        }

        val queryIndex = address.indexOf('?')
        if (queryIndex != -1) {
            address = address.substring(0, queryIndex)
        }

        return address
    }

}