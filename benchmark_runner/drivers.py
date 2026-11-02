"""Native, one-connection-per-client MongoDB and PostgreSQL drivers."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
QUERY_FILES = {
    "Q1": (ROOT / "queries/mongodb/q1.pipeline.json", ROOT / "queries/postgresql/q1.sql"),
    "Q2": (ROOT / "queries/mongodb/q2.pipeline.json", ROOT / "queries/postgresql/q2.sql"),
}


def treatment(engine: str, query_id: str) -> dict[str, Any]:
    matrix = json.loads((ROOT / "config/matrix.json").read_text(encoding="utf-8"))
    return matrix["q2_treatments"][engine].get(query_id, {})


def query_path(engine: str, query_id: str) -> Path:
    base_query = "Q2" if query_id == "Q2_ALT" else query_id
    return QUERY_FILES[base_query][0 if engine == "mongodb" else 1]


def query_definition(engine: str, query_id: str) -> Any:
    path = query_path(engine, query_id)
    if engine == "mongodb":
        pipeline = json.loads(path.read_text(encoding="utf-8"))
        return pipeline
    return path.read_text(encoding="utf-8")


class MongoWorker:
    def __init__(self, client: Any, collection: Any, query_id: str, run_id: str):
        self.client = client
        self.collection = collection
        self.query_id = query_id
        self.run_id = run_id
        self.pipeline = query_definition("mongodb", query_id)
        self.allow_disk_use = bool(treatment("mongodb", query_id).get("allowDiskUse", False))

    @classmethod
    def connect(cls, env: dict[str, str], query_id: str, run_id: str, client_id: int):
        from pymongo import MongoClient

        client = MongoClient(
            host=env.get("MONGO_HOST", "127.0.0.1"),
            port=int(env.get("MONGO_HOST_PORT", "27017")),
            username=env["MONGO_INITDB_ROOT_USERNAME"],
            password=env["MONGO_INITDB_ROOT_PASSWORD"],
            authSource="admin",
            appname=run_id[:63],
            maxPoolSize=1,
            minPoolSize=1,
            connectTimeoutMS=10000,
            serverSelectionTimeoutMS=10000,
            socketTimeoutMS=150000,
            waitQueueTimeoutMS=10000,
        )
        client.admin.command("ping")
        collection = client[env.get("MONGO_DB", "security_benchmark")][env.get("MONGO_COLLECTION", "events")]
        return cls(client, collection, query_id, run_id)

    def execute(self) -> int:
        cursor = self.collection.aggregate(
            self.pipeline,
            allowDiskUse=self.allow_disk_use,
            comment=self.run_id,
            maxTimeMS=120000,
        )
        try:
            return sum(1 for _ in cursor)
        finally:
            cursor.close()

    def server_version(self) -> str:
        return str(self.client.server_info()["version"])

    def effective_settings(self) -> dict[str, Any]:
        parsed = self.client.admin.command("getCmdLineOpts").get("parsed", {})
        storage = parsed.get("storage", {})
        wired = storage.get("wiredTiger", {}).get("engineConfig", {})
        network = parsed.get("net", {})
        return {
            "wiredTiger_cache_size_gb": wired.get("cacheSizeGB"),
            "max_incoming_connections": network.get("maxIncomingConnections"),
            "max_incoming_connections": parsed.get("net", {}).get("maxIncomingConnections"),
            "setParameter": parsed.get("setParameter", {}),
            "operationProfiling": parsed.get("operationProfiling", {}),
        }

    def close(self) -> None:
        self.client.close()

    def cancel(self) -> None:
        self.client.close()


class PostgreSQLWorker:
    def __init__(self, connection: Any, query_id: str, run_id: str):
        self.connection = connection
        self.query_id = query_id
        self.run_id = run_id
        self.sql = query_definition("postgresql", query_id)
        self.work_mem = str(treatment("postgresql", query_id).get("work_mem", "4MB"))

    @classmethod
    def connect(cls, env: dict[str, str], query_id: str, run_id: str, client_id: int):
        import psycopg

        connection = psycopg.connect(
            host=env.get("POSTGRES_HOST", "127.0.0.1"),
            port=int(env.get("POSTGRES_HOST_PORT", "5432")),
            dbname=env.get("POSTGRES_DB", "security_benchmark"),
            user=env["POSTGRES_USER"],
            password=env["POSTGRES_PASSWORD"],
            connect_timeout=10,
            application_name=run_id[:63],
            autocommit=True,
        )
        worker = cls(connection, query_id, run_id)
        # Fixed session defaults are applied to every physical client connection.
        connection.execute(f"SET work_mem = '{worker.work_mem}'")
        connection.execute("SET statement_timeout = '120s'")
        observed = connection.execute("SHOW work_mem").fetchone()[0]
        if observed.lower() != worker.work_mem.lower():
            connection.close()
            raise RuntimeError(f"work_mem efectivo {observed}; se esperaba {worker.work_mem}")
        return worker

    def execute(self) -> int:
        with self.connection.cursor() as cursor:
            cursor.execute(self.sql)
            count = 0
            while True:
                rows = cursor.fetchmany(512)
                if not rows:
                    break
                count += len(rows)
            return count

    def server_version(self) -> str:
        return str(self.connection.execute("SELECT version()").fetchone()[0])

    def effective_settings(self) -> dict[str, str]:
        names = (
            "shared_buffers", "effective_cache_size", "hash_mem_multiplier",
            "max_parallel_workers", "max_parallel_workers_per_gather", "work_mem",
            "statement_timeout", "max_connections", "log_temp_files", "log_line_prefix",
        )
        result: dict[str, str] = {}
        for name in names:
            result[name] = str(self.connection.execute(f"SHOW {name}").fetchone()[0])
        return result

    def close(self) -> None:
        self.connection.close()

    def connection_usable(self) -> bool:
        """Do not spin on a backend that was killed or disconnected."""
        try:
            return not bool(self.connection.closed)
        except Exception:
            return False

    def cancel(self) -> None:
        try:
            self.connection.cancel()
        except Exception:
            pass


def connect_worker(engine: str, env: dict[str, str], query_id: str, run_id: str, client_id: int):
    if engine == "mongodb":
        return MongoWorker.connect(env, query_id, run_id, client_id)
    return PostgreSQLWorker.connect(env, query_id, run_id, client_id)
