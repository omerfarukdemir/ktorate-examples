package io.github.omerfarukdemir.ktorate_examples

import com.google.gson.Gson
import io.github.omerfarukdemir.ktorate.Ktorate
import io.github.omerfarukdemir.ktorate.limiters.FixedWindow
import io.github.omerfarukdemir.ktorate.models.FixedWindowModel
import io.github.omerfarukdemir.ktorate.storages.FixedWindowStorage
import io.ktor.server.application.*
import io.ktor.server.engine.*
import io.ktor.server.netty.*
import io.ktor.server.response.*
import io.ktor.server.routing.*
import kotlin.time.Duration.Companion.hours
import redis.clients.jedis.JedisPool

fun main() {
    embeddedServer(Netty, module = Application::jedis).start(wait = true)
}

class RedisFixedWindowStorage : FixedWindowStorage {

    private val redisPool = JedisPool()
    private val gson = Gson()

    override suspend fun get(id: String): FixedWindowModel? {
        return redisPool.resource
            .use { it.get(id) }
            ?.let { gson.fromJson(it, FixedWindowModel::class.java) }
    }

    override suspend fun upsert(model: FixedWindowModel): FixedWindowModel {
        return model.also { redisPool.resource.use { it.set(model.id, gson.toJson(model)) } }
    }

    override suspend fun all(): Collection<FixedWindowModel> {
        val keys = redisPool.resource.use { it.keys("*") }

        return if (keys.isEmpty()) listOf()
        else redisPool.resource
            .use { it.mget(*keys.toTypedArray()) }
            .map { gson.fromJson(it, FixedWindowModel::class.java) }
    }

    override suspend fun delete(id: String): Boolean {
        return redisPool.resource.use { it.del(id) == 1L }
    }

    override suspend fun delete(ids: Collection<String>): Int {
        return redisPool.resource.use { it.del(*ids.toTypedArray()).toInt() }
    }
}

fun Application.jedis() {
    install(Ktorate) {
        rateLimiter = FixedWindow(
            duration = 1.hours,
            limit = 1000,
            synchronizedReadWrite = true,
            storage = RedisFixedWindowStorage()
        )
    }

    routing { get("/") { call.respondText("Hello World!") } }
}
