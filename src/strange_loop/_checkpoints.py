"""Transactional run metadata, immutable versions and append-only trace records."""

from __future__ import annotations

import base64
import json
import sqlite3
from collections.abc import Mapping
from contextlib import closing
from functools import cached_property
from pathlib import Path
from typing import TYPE_CHECKING, Any

from .codecs import Codecs

if TYPE_CHECKING:
    from .loop import Loop
    from .versions import Bindings


class StoredValue:
    def __init__(self, store: Checkpoints, run: str, version: str) -> None:
        self.store, self.run, self.version = store, run, version

    @cached_property
    def payload(self) -> tuple[str, bytes]:
        with closing(sqlite3.connect(self.store.path)) as db:
            name, data = db.execute(
                "SELECT codec, payload FROM versions WHERE run=? AND id=?",
                (self.run, self.version),
            ).fetchone()
        return name, data

    @property
    def value(self) -> Any:
        return self.store.codecs.decode(*self.payload)


class StoredAttachments(Mapping):
    def __init__(self, store, run, version, names):
        self.store = store
        self.run = run
        self.version = version
        self.names = names

    def __iter__(self):
        return iter(self.names)

    def __len__(self):
        return len(self.names)

    def __getitem__(self, key):
        if key not in self.names:
            raise KeyError(key)
        with closing(sqlite3.connect(self.store.path)) as db:
            codec, data = db.execute(
                "SELECT codec,payload FROM attachments "
                "WHERE run=? AND version=? AND name=?",
                (self.run, self.version, key),
            ).fetchone()
        return self.store.codecs.decode(codec, data)


def outcome_record(value):
    from .versions import Failure, MapResult, StaleVersion, Version

    if isinstance(value, Version):
        return value.id
    if isinstance(value, MapResult):
        return {"members": {k: outcome_record(v) for k, v in value.items()}}
    if isinstance(value, Failure):
        record = {
            "key": value.key,
            "kind": value.kind,
            "reason": value.reason,
            "target": value.target.id if value.target else None,
            "evidence": outcome_record(value.evidence)
            if isinstance(value.evidence, Version)
            else None,
            "feedback": value.evidence if isinstance(value.evidence, str) else None,
            "causes": {k: outcome_record(v) for k, v in (value.causes or {}).items()},
        }
        if isinstance(value, StaleVersion):
            record["current"] = value.current.id
        return record
    return None


def _restore_outcome(data, versions):
    from .versions import Failure, MapResult, StaleVersion

    if isinstance(data, str):
        return versions[data]
    if "members" in data:
        return MapResult(
            (k, _restore_outcome(v, versions)) for k, v in data["members"].items()
        )
    if "current" in data:
        return StaleVersion(
            data["key"],
            data["reason"],
            target=versions[data["target"]],
            current=versions[data["current"]],
            evidence=versions.get(data["evidence"]) or data["feedback"],
        )
    return Failure(
        data["key"],
        data["reason"],
        data["kind"],
        versions.get(data["target"]),
        versions.get(data["evidence"]) or data["feedback"],
        {k: _restore_outcome(v, versions) for k, v in data["causes"].items()} or None,
    )


class Checkpoints:
    def __init__(self, path: str | Path, codecs: Codecs) -> None:
        self.path = path
        self.codecs = codecs
        with closing(sqlite3.connect(path)) as db, db:
            db.executescript("""
                CREATE TABLE IF NOT EXISTS runs (
                    id TEXT PRIMARY KEY, generation INTEGER NOT NULL, metadata TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS versions (
                    run TEXT, id TEXT, artifact TEXT, actor TEXT, bindings TEXT, previous TEXT,
                    adopted TEXT, request TEXT, slot TEXT, codec TEXT, payload BLOB,
                    PRIMARY KEY(run,id));
                CREATE TABLE IF NOT EXISTS attachments (
                    run TEXT, version TEXT, name TEXT, codec TEXT, payload BLOB,
                    PRIMARY KEY(run,version,name));
                CREATE TABLE IF NOT EXISTS events (
                    run TEXT, seq INTEGER, subject TEXT, message TEXT, flow TEXT, stage TEXT,
                    PRIMARY KEY(run,seq));
                CREATE TABLE IF NOT EXISTS batches (
                    run TEXT, seq INTEGER, stage TEXT, keys TEXT, PRIMARY KEY(run,seq));
            """)

    def bindings(self, bindings: Bindings) -> str:
        result = []
        for name, bound in bindings:
            if bound[0] == "encoded":
                bound = ("encoded", bound[1], base64.b64encode(bound[2]).decode())
            result.append((name, bound))
        return json.dumps(result)

    def restore_bindings(self, data: str) -> Bindings:
        result = []
        for name, bound in json.loads(data):
            if bound[0] == "encoded":
                bound = ("encoded", bound[1], base64.b64decode(bound[2]))
            elif bound[0] == "members":
                bound = ("members", tuple(map(tuple, bound[1])))
            result.append((name, tuple(bound)))
        return tuple(result)

    def write(self, run: Loop) -> None:
        from .versions import Version

        key = run.run_id

        def ids(mapping):
            return {k: outcome_record(v) for k, v in mapping.items()}

        metadata = json.dumps(
            {
                "attempts": run.attempts,
                "limit": run.max_revisions,
                "mapped": run._mapped,
                "flow": run._flow_id,
                "collections": [
                    [
                        path,
                        identity,
                        ordinal,
                        self.bindings(inputs),
                        outcome_record(items),
                    ]
                    for (
                        path,
                        identity,
                        ordinal,
                        inputs,
                    ), items in run._collections.items()
                ],
                "roots": ids(run._roots),
                "finished": ids(run._finished),
                "iterations": run._iterations,
                "failures": [
                    [k, refs, outcome_record(v)]
                    for (k, refs), v in run._failures.items()
                ],
                "status": run.status,
                "result": outcome_record(run.result),
            }
        )
        with closing(sqlite3.connect(self.path)) as db, db:
            if run._generation == 0:
                try:
                    db.execute("INSERT INTO runs VALUES (?,1,?)", (key, metadata))
                except sqlite3.IntegrityError as error:
                    raise ValueError(f"Run already exists: {key}") from error
            elif not db.execute(
                "UPDATE runs SET generation=generation+1, metadata=? "
                "WHERE id=? AND generation=?",
                (metadata, key, run._generation),
            ).rowcount:
                raise RuntimeError(
                    "Run changed in another session. Reload before resuming."
                )

            for ref in run._new_versions:
                codec, data = ref._value.payload
                for name, item in ref._attachments.snapshots.items():
                    db.execute(
                        "INSERT INTO attachments VALUES (?,?,?,?,?)",
                        (key, ref.id, name, *item.payload),
                    )
                request = None
                if ref.request:
                    feedback = ref.request.feedback
                    request = [
                        ref.request.target.id,
                        isinstance(feedback, Version),
                        feedback.id if isinstance(feedback, Version) else feedback,
                    ]
                db.execute(
                    "INSERT INTO versions VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                    (
                        key,
                        ref.id,
                        ref.artifact,
                        ref.actor,
                        self.bindings(ref.inputs),
                        ref.previous,
                        ref.adopted_from,
                        json.dumps(request),
                        json.dumps(run._new_slots.get(ref.id)),
                        codec,
                        data,
                    ),
                )
            for seq in range(run._saved_events, len(run._events)):
                event = run._events[seq]
                db.execute(
                    "INSERT INTO events VALUES (?,?,?,?,?,?)",
                    (
                        key,
                        seq,
                        event.key,
                        event.message,
                        json.dumps(event.flow),
                        json.dumps(event.stage),
                    ),
                )
            for seq in range(run._saved_batches, len(run.batches)):
                stage, keys = run.batches[seq]
                db.execute(
                    "INSERT INTO batches VALUES (?,?,?,?)",
                    (key, seq, stage, json.dumps(keys)),
                )
        run._generation += 1
        run._new_versions.clear()
        run._new_slots.clear()
        run._saved_events = len(run._events)
        run._saved_batches = len(run.batches)

    def read(self, key: str, run: Loop) -> None:
        from .versions import MapResult, Revise, TraceEvent, Version

        with closing(sqlite3.connect(self.path)) as db:
            db.execute("BEGIN")
            row = db.execute(
                "SELECT generation,metadata FROM runs WHERE id=?", (key,)
            ).fetchone()
            if row is None:
                raise KeyError(key)
            run._generation, metadata = row
            meta = json.loads(metadata)
            run.run_id = key
            run.attempts, run.max_revisions = meta["attempts"], meta["limit"]
            run._mapped, run._flow_id = meta["mapped"], tuple(meta["flow"])
            run.status = meta["status"]
            run._iterations.update(meta["iterations"])
            attachments = {}
            for vid, name in db.execute(
                "SELECT version,name FROM attachments WHERE run=?", (key,)
            ):
                attachments.setdefault(vid, []).append(name)
            rows = db.execute(
                "SELECT id,artifact,actor,bindings,previous,adopted,request,slot "
                "FROM versions WHERE run=? ORDER BY rowid",
                (key,),
            )
            for (
                vid,
                artifact,
                actor,
                bindings,
                previous,
                adopted,
                request,
                slot,
            ) in rows:
                inputs = self.restore_bindings(bindings)
                revision = None
                if details := json.loads(request):
                    target, is_ref, feedback = details
                    revision = Revise(
                        run.versions[target],
                        run.versions[feedback] if is_ref else feedback,
                    )
                ref = Version._recorded(
                    vid,
                    artifact,
                    StoredValue(self, key, vid),
                    actor,
                    inputs,
                    previous,
                    adopted,
                    revision,
                    StoredAttachments(self, key, vid, attachments.get(vid, ())),
                )
                run.versions[vid] = run.current[artifact] = ref
                path, name, number = artifact.rsplit("/", 2)
                run._counts[path, name] = max(run._counts[path, name], int(number))
                if call := json.loads(slot):
                    subject, identity, ordinal = call
                    run.slots[subject, tuple(identity), ordinal, inputs] = artifact

            run._failures = {
                (k, tuple(refs)): _restore_outcome(v, run.versions)
                for k, refs, v in meta.get("failures", [])
            }
            for path, identity, ordinal, bindings, items in meta.get("collections", []):
                slot = (path, tuple(identity), ordinal, self.restore_bindings(bindings))
                run._collections[slot] = _restore_outcome(items, run.versions)
            run._roots = {
                k: _restore_outcome(v, run.versions) for k, v in meta["roots"].items()
            }
            run._finished = {
                k: _restore_outcome(v, run.versions)
                for k, v in meta["finished"].items()
            }
            if run.status == "completed":
                run.result = (
                    MapResult((k, run._finished[k]) for k in run._roots)
                    if run._mapped
                    else _restore_outcome(meta["result"], run.versions)
                )
            for subject, message, flow, stage in db.execute(
                "SELECT subject,message,flow,stage FROM events WHERE run=? ORDER BY seq",
                (key,),
            ):
                run._events.append(
                    TraceEvent(
                        subject,
                        message,
                        tuple(json.loads(flow)) if flow != "null" else None,
                        tuple(json.loads(stage)) if stage != "null" else None,
                    )
                )
            run.batches = [
                (stage, tuple(json.loads(keys)))
                for stage, keys in db.execute(
                    "SELECT stage,keys FROM batches WHERE run=? ORDER BY seq", (key,)
                )
            ]
            run._saved_events, run._saved_batches = len(run._events), len(run.batches)

    def keys(self) -> list[str]:
        with closing(sqlite3.connect(self.path)) as db:
            return [row[0] for row in db.execute("SELECT id FROM runs ORDER BY rowid")]
