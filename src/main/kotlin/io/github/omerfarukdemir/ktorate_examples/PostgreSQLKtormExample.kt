package io.github.omerfarukdemir.ktorate_examples

import io.github.omerfarukdemir.ktorate.Ktorate
import io.github.omerfarukdemir.ktorate.limiters.FixedWindow
import io.github.omerfarukdemir.ktorate.models.FixedWindowModel
import io.github.omerfarukdemir.ktorate.storages.FixedWindowStorage
import io.ktor.server.application.Application
import io.ktor.server.application.call
import io.ktor.server.application.install
import io.ktor.server.engine.embeddedServer
import io.ktor.server.netty.Netty
import io.ktor.server.response.respondText
import io.ktor.server.routing.get
import io.ktor.server.routing.routing
import kotlin.time.Duration.Companion.hours
import org.ktorm.database.Database
import org.ktorm.schema.Table
import org.ktorm.schema.int
import org.ktorm.schema.varchar
import org.ktorm.support.postgresql.insertOrUpdate
import org.ktorm.dsl.QueryRowSet
import org.ktorm.dsl.delete
import org.ktorm.dsl.eq
import org.ktorm.dsl.from
import org.ktorm.dsl.inList
import org.ktorm.dsl.limit
import org.ktorm.dsl.map
import org.ktorm.dsl.select
import org.ktorm.dsl.where

fun main() {
    embeddedServer(Netty, module = Application::ktorm).start(wait = true)
}

object KtormFixedWindowTable : Table<Nothing>("fixed_window") {
    val id = varchar("id").primaryKey()
    val startInSeconds = int("start_in_seconds")
    val requestCount = int("request_count")
}

class KtormFixedWindowStorage : FixedWindowStorage {

    // make sure schema is ready before run this example
    // CREATE DATABASE ktorate;
    // CREATE TABLE fixed_window (id varchar NOT NULL PRIMARY KEY, start_in_seconds INT NOT NULL, request_count INT NOT NULL);

    private val database = Database.connect(
        url = "jdbc:postgresql://localhost:5432/ktorate",
        user = "postgres",
        password = "postgres"
    )

    override suspend fun get(id: String): FixedWindowModel? {
        return database.from(KtormFixedWindowTable)
            .select()
            .where(KtormFixedWindowTable.id eq id)
            .limit(1)
            .map { it.fixedWindowModel() }
            .firstOrNull()
    }

    override suspend fun upsert(model: FixedWindowModel): FixedWindowModel {
        return model.also {
            database.insertOrUpdate(KtormFixedWindowTable) {
                set(it.id, model.id)
                set(it.startInSeconds, model.startInSeconds)
                set(it.requestCount, model.requestCount)
                onConflict {
                    set(it.startInSeconds, model.startInSeconds)
                    set(it.requestCount, model.requestCount)
                }
            }
        }
    }

    override suspend fun all(): Collection<FixedWindowModel> {
        return database.from(KtormFixedWindowTable)
            .select()
            .map { it.fixedWindowModel() }
    }

    override suspend fun delete(id: String): Boolean {
        return database.delete(KtormFixedWindowTable) { it.id eq id } == 1
    }

    override suspend fun delete(ids: Collection<String>): Int {
        return database.delete(KtormFixedWindowTable) { it.id inList ids }
    }

    private fun QueryRowSet.fixedWindowModel(): FixedWindowModel {
        return FixedWindowModel(
            this[KtormFixedWindowTable.id]!!,
            this[KtormFixedWindowTable.startInSeconds]!!,
            this[KtormFixedWindowTable.requestCount]!!
        )
    }
}

fun Application.ktorm() {
    install(Ktorate) {
        rateLimiter = FixedWindow(
            duration = 1.hours,
            limit = 1000,
            synchronizedReadWrite = true,
            storage = KtormFixedWindowStorage()
        )
    }

    routing { get("/") { call.respondText("Hello World!") } }
}
