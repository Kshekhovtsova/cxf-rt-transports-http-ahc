package ru.yuksanbo.cxf.transportahc

import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import io.netty.channel.ChannelOption
import io.netty.channel.EventLoopGroup
import io.netty.handler.ssl.SslContext
import io.netty.util.HashedWheelTimer
import io.netty.util.Timer
import org.asynchttpclient.AsyncHttpClientConfig
import org.asynchttpclient.DefaultAsyncHttpClientConfig
import org.slf4j.LoggerFactory
import ru.yuksanbo.common.security.Ssl

private val defaultTimer by lazy { HashedWheelTimer() }
private val log = LoggerFactory.getLogger(AhcHttpConduitConfig::class.java)

class AhcHttpConduitConfig(
        val httpClientConfig: AsyncHttpClientConfig,
        val timingsThreshold: Long,
        val bufferPerRequestSize: Long
) {

    companion object {

        //todo: parse map
        @JvmOverloads
        @JvmStatic
        private fun from(
                config: MutableMap<String?, String?>,
                defaults: Config,
                sslContext: SslContext? = null,
                eventLoopGroup: EventLoopGroup? = null,
                timer: Timer? = null
        ): AhcHttpConduitConfig = from(
                ConfigFactory.parseMap(config).withFallback(defaults).resolve(),
                sslContext,
                eventLoopGroup,
                timer
        )

        @JvmOverloads
        @JvmStatic
        fun from(
                config: Config,
                sslContext: SslContext? = null,
                eventLoopGroup: EventLoopGroup? = null,
                timer: Timer? = null
        ): AhcHttpConduitConfig {
            val timingsThreshold = config.getDuration("timings-threshold").toMillis()
            val httpClientConfigBuilder = builderFromConfig(config)

            /* ssl context configuration */
            if (sslContext == null) {
                if (config.getBoolean("ssl.enabled")) {
                    val kstoreConfig = config.getConfig("ssl.keystore")
                    val keystorePath = kstoreConfig.getString("path")
                    assert(!keystorePath.isBlank(), { "ssl.keystore.path property can't be blank" })
                    httpClientConfigBuilder.setSslContext(
                            Ssl.buildNettyClientSslContext(
                                    keystorePath,
                                    kstoreConfig.getString("type"),
                                    kstoreConfig.getString("password")
                            )
                    )
                }
            }
            else {
                httpClientConfigBuilder.setSslContext(sslContext)
            }

            //todo: allow to set user channel pool? or configure channel pool lease strategy (lifo, fifo)

            /* thread pool configuration */
            eventLoopGroup?.let {
                httpClientConfigBuilder.setEventLoopGroup(it)
            }

            httpClientConfigBuilder.setNettyTimer(timer ?: defaultTimer)

            return AhcHttpConduitConfig(
                    httpClientConfigBuilder.build(),
                    timingsThreshold,
                    config.getMemorySize("buffer-per-request-size").toBytes()
            )
        }

        private fun builderFromConfig(config: Config): DefaultAsyncHttpClientConfig.Builder {
            /* connection pool config */
            val connPoolConfig = config.getConfig("connection-pool")

            return DefaultAsyncHttpClientConfig.Builder()
                    .addChannelOption(ChannelOption.AUTO_CLOSE, config.getBoolean("auto-close-on-ioexception"))
                    .setShutdownQuietPeriod(config.getDuration("shutdown.quiet-period").toMillis().toInt())
                    .setShutdownTimeout(config.getDuration("shutdown.timeout").toMillis().toInt())
                    .setKeepAlive(config.getBoolean("keep-alive"))
                    .setUserAgent(config.getString("user-agent"))
                    .setMaxRequestRetry(config.getInt("max-request-retry"))
                    .setRequestTimeout(config.getDuration("request-timeout").toMillis().toInt())
                    .setConnectTimeout(config.getDuration("connect-timeout").toMillis().toInt())
                    .setReadTimeout(config.getDuration("read-timeout").toMillis().toInt())
                    .setHandshakeTimeout(config.getDuration("handshake-timeout").toMillis().toInt())
                    .setIoThreadsCount(config.getInt("io-threads-count"))
                    .setThreadPoolName(config.getString("io-thread-pool-name"))
                    .setPooledConnectionIdleTimeout(connPoolConfig.getDuration("idle-connection-timeout").toMillis().toInt())
                    .setConnectionTtl(connPoolConfig.getDuration("connection-ttl").toMillis().toInt())
                    .setMaxConnections(connPoolConfig.getInt("max-connections"))
                    .setMaxConnectionsPerHost(connPoolConfig.getInt("max-connections-per-host"))
                    .setSslSessionTimeout(config.getDuration("ssl.session-timeout").toMillis().toInt())
        }
    }
}