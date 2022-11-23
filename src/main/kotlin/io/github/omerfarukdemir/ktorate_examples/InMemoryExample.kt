package io.github.omerfarukdemir.ktorate_examples

import io.github.omerfarukdemir.ktorate.Ktorate
import io.github.omerfarukdemir.ktorate.limiters.FixedWindow
import io.ktor.server.application.*
import io.ktor.server.engine.*
import io.ktor.server.netty.*
import io.ktor.server.response.respondText
import io.ktor.server.routing.get
import io.ktor.server.routing.routing
import kotlin.time.Duration.Companion.hours
import kotlin.time.Duration.Companion.seconds

fun main() {
    embeddedServer(Netty, module = Application::inMemory).start(wait = true)
}

fun Application.inMemory() {
    install(Ktorate) {
        // to remove expired records in data store
        deleteExpiredRecordsPeriod = 5.seconds

        rateLimiter = FixedWindow(
            // strategy window
            duration = 1.hours,

            // max request in duration by defined strategy
            limit = 1000,

            // blocking ops between read and write ops (only for same identity)
            synchronizedReadWrite = true
        )

        // count starting path with "/v1/api/" urls
        includedPaths = listOf(Regex("^/api/v1/.*$"))

        // do not count .html urls
        excludedPaths = listOf(Regex("^.*html$"))
    }

    routing { get("/") { call.respondText("Hello World!") } }
}
