from __future__ import annotations
"""mpmq/broker.py

Core broker logic for MPMQ.
Paired modules expected in package:
    • mpmq.storage.file_segment – persistence helpers
    • mpmq.metrics             – Prometheus counters
    • mpmq.client              – producer/consumer SDK (imports Broker only for typing)

This file focuses on Topics, Partitions, and Group‑offset tracking.
A **single‑node, single‑process** implementation for clarity; horizontally
scalable patterns are noted inline as TODOs.
"""
import asyncio
import os
import time
import sqlite3
from pathlib import Path
from typing import Dict, List

from .metrics import PUBLISHED, CONSUMED  # type: ignore
from .storage.file_segment import FileSegment  # type: ignore

__all__ = ["Broker"]

SEGMENT_BYTES = 1 << 20  # 1 MiB demo segment size

# ─────────────────────────── Topic / Partition ────────────────────────────────
class Partition:
    """Manages a chain of log segments (append‑only)."""

    def __init__(self, topic: str, pid: int, data_dir: Path):
        self.topic, self.pid = topic, pid
        self.dir = data_dir / topic / str(pid)
        self.dir.mkdir(parents=True, exist_ok=True)
        self.segments: List[FileSegment] = self._load_segments()
        self._lock = asyncio.Lock()

    # ‑‑ internal helpers ‑‑
    def _load_segments(self) -> List[FileSegment]:
        existing = sorted(self.dir.glob("seg_*.log"))
        segments: List[FileSegment] = []
        base = 0
        for fp in existing:
            seg = FileSegment(fp, base)
            segments.append(seg)
            base = seg.next_offset
        if not segments:
            seg_path = self.dir / "seg_0.log"
            segments.append(FileSegment(seg_path, 0))
        return segments

    @property
    def head(self) -> FileSegment:  # newest/open segment
        return self.segments[-1]

    # ‑‑ public API ‑‑
    async def append(self, value: bytes) -> int:
        async with self._lock:
            offset = self.head.next_offset
            record = {
                "offset": offset,
                "ts": int(time.time() * 1000),
                "value": value.hex(),
            }
            try:
                self.head.append(record)
            except IOError:
                # roll segment when full
                new_path = self.dir / f"seg_{offset}.log"
                new_seg = FileSegment(new_path, offset)
                self.segments.append(new_seg)
                new_seg.append(record)
            self.head.next_offset += 1
            return offset

    async def read(self, start_offset: int, max_records: int) -> List[dict]:
        out: List[dict] = []
        for seg in self.segments:
            if start_offset >= seg.base_offset + seg.count:
                continue
            batch = seg.read_from(start_offset - seg.base_offset, max_records - len(out))
            out.extend(batch)
            if len(out) >= max_records:
                break
        return out

class Topic:
    def __init__(self, name: str, partitions: int, data_dir: Path):
        self.name = name
        self.partitions: List[Partition] = [Partition(name, i, data_dir) for i in range(partitions)]
        self._rr = 0  # round‑robin pointer

    def next_partition(self) -> Partition:
        part = self.partitions[self._rr]
        self._rr = (self._rr + 1) % len(self.partitions)
        return part

# ───────────────────────────── Broker core ────────────────────────────────────
class Broker:
    """Single‑node async broker with group‑offset persistence (SQLite)."""

    def __init__(self, data_dir: str | os.PathLike = "./data", db: str | os.PathLike = "offsets.db"):
        self.data_dir = Path(data_dir)
        self.topics: Dict[str, Topic] = {}
        self.db = sqlite3.connect(db, check_same_thread=False)
        self._db_lock = asyncio.Lock()
        self._init_db()

    # ‑‑ DB bootstrap ‑‑
    def _init_db(self):
        cur = self.db.cursor()
        cur.execute(
            """CREATE TABLE IF NOT EXISTS offsets (
                   topic TEXT,
                   partition INTEGER,
                   grp TEXT,
                   next_offset INTEGER,
                   PRIMARY KEY(topic, partition, grp)
               )"""
        )
        self.db.commit()

    # ‑‑ Admin ops ‑‑
    def create_topic(self, name: str, partitions: int = 1):
        if name not in self.topics:
            self.topics[name] = Topic(name, partitions, self.data_dir)

    # ‑‑ Data path ‑‑
    async def publish(self, topic: str, value: bytes) -> int:
        if topic not in self.topics:
            self.create_topic(topic)
        partition = self.topics[topic].next_partition()
        offset = await partition.append(value)
        PUBLISHED.inc()
        return offset

    async def consume_group(self, topic: str, group: str, max_records: int = 10) -> List[dict]:
        if topic not in self.topics:
            return []
        partition = self.topics[topic].partitions[0]  # single‑partition demo
        async with self._db_lock:
            cur = self.db.execute(
                "SELECT next_offset FROM offsets WHERE topic=? AND partition=? AND grp=?",
                (topic, partition.pid, group),
            )
            row = cur.fetchone()
            offset = row[0] if row else 0
            records = await partition.read(offset, max_records)
            if records:
                new_off = records[-1]["offset"] + 1
                self.db.execute(
                    "INSERT OR REPLACE INTO offsets VALUES (?,?,?,?)",
                    (topic, partition.pid, group, new_off),
                )
                self.db.commit()
                CONSUMED.inc(len(records))
            return records

