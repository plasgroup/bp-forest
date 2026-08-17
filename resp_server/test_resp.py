#!/usr/bin/env python3
"""Functional test for bp-forest resp_server against a Python dict model.

Spawns the server binary (RESP_SERVER env var, or the fake_dpu build under
build/resp_fake/ by default), then checks every command against a serially
applied dict model: basic semantics and error replies, a 4000-command mixed
pipeline on one connection (serial consistency across epoch cuts), 4
concurrent pipelining connections, a 100k-command deep pipeline, and a rerun
of everything with a small --batch-size to exercise epoch splitting.
"""

import os
import random
import socket
import subprocess
import sys
import threading
import time

SERVER = os.environ.get(
    "RESP_SERVER",
    os.path.normpath(os.path.join(os.path.dirname(os.path.abspath(__file__)),
        "..", "build", "resp_fake", "resp_server", "resp_server_fake_dpu")))

INIT_NR = 100000
INIT_STRIDE = 4  # keys 4, 8, ..., 400000; value = key

failures = []


def check(name, got, want):
    if got != want:
        failures.append(f"{name}: got {got!r}, want {want!r}")
        print(f"FAIL {name}: got {got!r}, want {want!r}")


# ---------------- RESP client ----------------

class Client:
    def __init__(self, port):
        self.sock = socket.create_connection(("127.0.0.1", port))
        self.buf = b""

    def close(self):
        self.sock.close()

    def send_raw(self, data: bytes):
        self.sock.sendall(data)

    def send(self, *args):
        self.send_raw(encode(args))

    def _fill(self):
        data = self.sock.recv(1 << 16)
        if not data:
            raise EOFError("server closed connection")
        self.buf += data

    def _read_line(self):
        while True:
            pos = self.buf.find(b"\r\n")
            if pos >= 0:
                line, self.buf = self.buf[:pos], self.buf[pos + 2:]
                return line
            self._fill()

    def _read_exact(self, n):
        while len(self.buf) < n:
            self._fill()
        data, self.buf = self.buf[:n], self.buf[n:]
        return data

    def read_reply(self):
        line = self._read_line()
        t, rest = line[:1], line[1:]
        if t == b"+":
            return rest.decode()
        if t == b"-":
            return Exception(rest.decode())
        if t == b":":
            return int(rest)
        if t == b"$":
            n = int(rest)
            if n == -1:
                return None
            data = self._read_exact(n)
            self._read_exact(2)
            return data.decode()
        if t == b"*":
            n = int(rest)
            if n == -1:
                return None
            return [self.read_reply() for _ in range(n)]
        raise ValueError(f"bad reply line: {line!r}")

    def cmd(self, *args):
        self.send(*args)
        return self.read_reply()

    def pipeline(self, cmds):
        self.send_raw(b"".join(encode(c) for c in cmds))
        return [self.read_reply() for _ in cmds]


def encode(args):
    out = [f"*{len(args)}\r\n".encode()]
    for a in args:
        b = str(a).encode()
        out.append(f"${len(b)}\r\n".encode() + b + b"\r\n")
    return b"".join(out)


def is_err(v):
    return isinstance(v, Exception)


def unsupported(v):
    return is_err(v) and "not supported" in str(v)


def probe_support(c):
    """Detects the ops compiled into this build, via instant-path probes
    (an invalid key errors at admission, before any DPU is involved)."""
    sup = set()
    for op, probe in [
        ("get", ("GET", "x")),
        ("set", ("SET", "x", "1")),
        ("del", ("DEL", "x")),
        ("pred", ("BPF.PRED", "x")),
        ("count", ("BPF.RANGECOUNT", "x", "1", "1")),
        ("max", ("BPF.RANGEMAX", "x", "1")),
    ]:
        if not unsupported(c.cmd(*probe)):
            sup.add(op)
    return sup


# ---------------- model helpers ----------------

def model_pred(model, key):
    """Strict predecessor; scans downward (init stride keeps gaps small)."""
    for k in range(key - 1, max(0, key - 128), -1):
        if k in model:
            return [str(k), str(model[k])]
    cands = [k for k in model if k < key]  # rare fallback for a wide gap
    if not cands:
        return None
    k = max(cands)
    return [str(k), str(model[k])]


def model_count(model, b, e, needle):
    return sum(1 for k in range(b, e + 1) if model.get(k) == needle)


def model_max(model, b, e):
    vals = [model[k] for k in range(b, e + 1) if k in model]
    return str(max(vals)) if vals else None


# ---------------- test sections ----------------

def start_server(port, *extra):
    proc = subprocess.Popen(
        [SERVER, "--port", str(port), "--init-nr", str(INIT_NR),
         "--init-stride", str(INIT_STRIDE), *extra],
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    while True:
        line = proc.stdout.readline()
        if not line:
            raise RuntimeError("server exited before listening")
        if "listening" in line:
            return proc


def fresh_model():
    return {(i + 1) * INIT_STRIDE: (i + 1) * INIT_STRIDE for i in range(INIT_NR)}


def test_basic(port, sup):
    assert {"get", "set", "del"} <= sup, f"server build lacks the test minimum: {sup}"
    c = Client(port)
    check("ping", c.cmd("PING"), "PONG")
    check("ping msg", c.cmd("PING", "hello"), "hello")
    check("echo", c.cmd("ECHO", "abc"), "abc")
    check("command", c.cmd("COMMAND", "DOCS"), [])
    check("dbsize", c.cmd("DBSIZE"), INIT_NR)
    check("case insensitive", c.cmd("get", "4"), "4")

    check("get hit", c.cmd("GET", "8"), "8")
    check("get miss", c.cmd("GET", "9"), None)
    check("get badkey", is_err(c.cmd("GET", "abc")), True)
    check("get arity", is_err(c.cmd("GET")), True)
    check("unknown cmd", is_err(c.cmd("NOSUCH", "1")), True)

    check("set", c.cmd("SET", "9", "1009"), "OK")
    check("get after set", c.cmd("GET", "9"), "1009")
    check("set overwrite", c.cmd("SET", "9", "2009"), "OK")
    check("get after overwrite", c.cmd("GET", "9"), "2009")
    check("set zero value", is_err(c.cmd("SET", "10", "0")), True)

    check("exists", c.cmd("EXISTS", "4", "5", "8", "8"), 3)
    check("del", c.cmd("DEL", "9"), 1)
    check("get after del", c.cmd("GET", "9"), None)
    check("del missing", c.cmd("DEL", "9"), 0)
    check("del dup args", c.cmd("DEL", "12", "12"), 1)
    check("get after dup del", c.cmd("GET", "12"), None)
    c.cmd("SET", "12", "12")  # restore

    if "pred" in sup:
        # predecessor: keys 4,8,...; strict pred of 8 is (4,4); of 5 is (4,4)
        check("pred exact", c.cmd("BPF.PRED", "8"), ["4", "4"])
        check("pred between", c.cmd("BPF.PRED", "5"), ["4", "4"])
        check("pred min", c.cmd("BPF.PRED", "4"), None)
        check("pred below min", c.cmd("BPF.PRED", "1"), None)
        top = INIT_NR * INIT_STRIDE
        check("pred above max", c.cmd("BPF.PRED", str(top + 1000)), [str(top), str(top)])
    else:
        check("pred unsupported", unsupported(c.cmd("BPF.PRED", "8")), True)

    if "count" in sup:
        # range count: values == keys initially
        check("count hit", c.cmd("BPF.RANGECOUNT", "4", "40", "8"), 1)
        check("count nohit", c.cmd("BPF.RANGECOUNT", "4", "40", "9"), 0)
        check("count inverted", c.cmd("BPF.RANGECOUNT", "40", "4", "8"), 0)
    else:
        check("count unsupported", unsupported(c.cmd("BPF.RANGECOUNT", "4", "40", "8")), True)

    if "max" in sup:
        check("rangemax", c.cmd("BPF.RANGEMAX", "4", "41"), "40")
        check("rangemax empty", c.cmd("BPF.RANGEMAX", "9", "11"), None)
        check("rangemax inverted", c.cmd("BPF.RANGEMAX", "11", "9"), None)
    else:
        check("rangemax unsupported", unsupported(c.cmd("BPF.RANGEMAX", "4", "41")), True)

    if "pred" in sup:
        # pred over tombstones: the DPU delete keeps the key with value 0; the
        # server must walk down to the live predecessor transparently
        check("del for tombstone", c.cmd("DEL", "8"), 1)
        check("pred skips tombstone", c.cmd("BPF.PRED", "9"), ["4", "4"])
        check("del for tombstone 2", c.cmd("DEL", "4"), 1)
        check("pred only tombstones", c.cmd("BPF.PRED", "9"), None)
        check("resurrect tombstone", c.cmd("SET", "8", "88"), "OK")
        check("pred after resurrect", c.cmd("BPF.PRED", "9"), ["8", "88"])
        check("restore 4", c.cmd("SET", "4", "4"), "OK")
        check("restore 8", c.cmd("SET", "8", "8"), "OK")

    # DBSIZE counts live pairs, so a new key and its deletion both show up
    k = 70_000_001  # odd, so outside every other test's key range
    n0 = c.cmd("DBSIZE")
    check("set for dbsize", c.cmd("SET", k, 1), "OK")
    check("dbsize after set", c.cmd("DBSIZE"), n0 + 1)
    check("set again for dbsize", c.cmd("SET", k, 2), "OK")
    check("dbsize after overwrite", c.cmd("DBSIZE"), n0 + 1)
    check("del for dbsize", c.cmd("DEL", k), 1)
    check("dbsize after del", c.cmd("DBSIZE"), n0)

    # inline command
    c.send_raw(b"PING\r\n")
    check("inline ping", c.read_reply(), "PONG")

    check("quit", c.cmd("QUIT"), "OK")
    c.close()

    # protocol error closes the connection
    c2 = Client(port)
    c2.send_raw(b"*abc\r\n")
    r = c2.read_reply()
    check("protocol error", is_err(r), True)
    try:
        c2.read_reply()
        check("close after protocol error", "still open", "closed")
    except EOFError:
        pass
    c2.close()


def test_pipeline_serial(port, model, sup):
    """A single connection's pipelined commands must behave serially."""
    rng = random.Random(42)
    keyspace = [k for k in range(2, 2000)]
    c = Client(port)
    check("pin key1", c.cmd("SET", "1", "1"), "OK")
    model[1] = 1

    op_choices = ["SET", "SET", "GET", "GET", "DEL", "EXISTS", "PING"]
    for name, op in [("pred", "PRED"), ("count", "COUNT"), ("max", "MAX")]:
        if name in sup:
            op_choices.append(op)

    cmds, want = [], []
    for _ in range(4000):
        op = rng.choice(op_choices)
        k = rng.choice(keyspace)
        if op == "SET":
            v = rng.randrange(1, 1 << 30)
            cmds.append(("SET", k, v))
            model[k] = v
            want.append("OK")
        elif op == "GET":
            cmds.append(("GET", k))
            want.append(str(model[k]) if k in model else None)
        elif op == "DEL":
            k2 = rng.choice(keyspace)
            cmds.append(("DEL", k, k2))
            n = len({kk for kk in (k, k2) if kk in model})
            model.pop(k, None)
            model.pop(k2, None)
            want.append(n)
        elif op == "EXISTS":
            k2 = rng.choice(keyspace)
            cmds.append(("EXISTS", k, k2, k))
            want.append((k in model) + (k2 in model) + (k in model))
        elif op == "PRED":
            cmds.append(("BPF.PRED", k))
            want.append(model_pred(model, k))
        elif op == "COUNT":
            b = rng.choice(keyspace)
            e = b + rng.randrange(0, 50)
            probe = rng.choice(keyspace)
            needle = model.get(probe, probe)
            cmds.append(("BPF.RANGECOUNT", b, e, needle))
            want.append(model_count(model, b, e, needle))
        elif op == "MAX":
            b = rng.choice(keyspace)
            e = b + rng.randrange(0, 50)
            cmds.append(("BPF.RANGEMAX", b, e))
            want.append(model_max(model, b, e))
        else:
            cmds.append(("PING",))
            want.append("PONG")

    got = c.pipeline(cmds)
    nr_bad = 0
    for i, (g, w) in enumerate(zip(got, want)):
        if g != w:
            nr_bad += 1
            if nr_bad <= 5:
                print(f"FAIL pipeline[{i}] {cmds[i]}: got {g!r}, want {w!r}")
    if nr_bad:
        failures.append(f"pipeline_serial: {nr_bad}/{len(cmds)} mismatches")
    else:
        print(f"pipeline_serial: {len(cmds)} commands OK")
    c.close()


def test_multi_conn(port):
    """Concurrent pipelining connections on disjoint key ranges."""
    def worker(conn_id, results):
        rng = random.Random(conn_id)
        base = 10_000_000 * (conn_id + 1)
        model = {}
        c = Client(port)
        ops = []
        want = []
        for _ in range(3000):
            k = base + rng.randrange(0, 500)
            r = rng.random()
            if r < 0.5:
                v = rng.randrange(1, 1 << 30)
                ops.append(("SET", k, v))
                model[k] = v
                want.append("OK")
            elif r < 0.8:
                ops.append(("GET", k))
                want.append(str(model[k]) if k in model else None)
            else:
                ops.append(("DEL", k))
                want.append(1 if k in model else 0)
                model.pop(k, None)
        got = c.pipeline(ops)
        bad = sum(1 for g, w in zip(got, want) if g != w)
        results[conn_id] = bad
        c.close()

    results = {}
    threads = [threading.Thread(target=worker, args=(i, results)) for i in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    total_bad = sum(results.values())
    if total_bad:
        failures.append(f"multi_conn: {total_bad} mismatches {results}")
    else:
        print(f"multi_conn: 4 connections x 3000 commands OK")


def test_write_collision(port, sup):
    """SET and DEL of the same key raced from two connections.  Whether or not
    they land in the same epoch, the epoch order (inserts before deletes)
    serializes them as SET-then-DEL: the DEL must report 1 and the key must be
    gone.  The retired probe-based DEL replied 0 whenever both landed in one
    epoch, contradicting every serialization."""
    if "del" not in sup:
        return
    a = Client(port)
    b = Client(port)
    nr_bad = 0
    nr_rounds = 0
    t0 = time.time()
    # Every round costs a few epochs, which dominates on slow backends
    # (dpu_on_cpu), so run at least 20 rounds within a 60-second budget.
    while nr_rounds < 200 and (nr_rounds < 20 or time.time() - t0 < 60.0):
        k = 90_000_000 + nr_rounds
        a.send("SET", k, 7)
        b.send("DEL", k)
        got_set = a.read_reply()
        got_del = b.read_reply()
        got_get = a.cmd("GET", k)
        if got_set != "OK" or got_del != 1 or got_get is not None:
            nr_bad += 1
            if nr_bad <= 5:
                print(f"FAIL write_collision[{nr_rounds}]: SET={got_set!r} DEL={got_del!r} GET={got_get!r}")
        nr_rounds += 1
    a.close()
    b.close()
    if nr_bad:
        failures.append(f"write_collision: {nr_bad}/{nr_rounds} rounds bad")
    else:
        print(f"write_collision: {nr_rounds} rounds OK")


def test_deleted_run_pred(port, sup):
    """Deletes a run of adjacent fresh keys front-to-back while querying
    BPF.PRED just above the dead prefix.  Exercises tombstone skipping and,
    whenever a deleted key is a partition minimum, the min bookkeeping.  All
    checks are relative to a reference predecessor taken below the block, so
    the test is independent of what other tests left in the server."""
    if not {"del", "pred", "set"} <= sup:
        return
    c = Client(port)
    base = 70_000_000  # a key range no other test touches
    ks = [base + 4 * i for i in range(10)]
    for k in ks:
        check("run setup", c.cmd("SET", k, k), "OK")
    ref = c.cmd("BPF.PRED", ks[0])  # the block's predecessor, whatever it is
    for i, k in enumerate(ks[:-1]):
        check(f"del run {k}", c.cmd("DEL", k), 1)
        # everything of the block at or below k is dead: pred falls through
        check(f"pred skips run upto {k}", c.cmd("BPF.PRED", k + 1), ref)
        check(f"pred next alive {k}", c.cmd("BPF.PRED", ks[i + 1] + 1), [str(ks[i + 1]), str(ks[i + 1])])
    check("del run last", c.cmd("DEL", ks[-1]), 1)
    check("pred whole run dead", c.cmd("BPF.PRED", ks[-1] + 1), ref)
    check("run resurrect", c.cmd("SET", ks[3], 1616), "OK")
    check("pred resurrected", c.cmd("BPF.PRED", ks[-1] + 1), [str(ks[3]), "1616"])
    c.close()


def test_throughput(port):
    """Deep-pipeline sanity: bulk SETs then GETs on one connection."""
    n = 100000
    base = 50_000_000
    c = Client(port)
    t0 = time.time()
    got = c.pipeline([("SET", base + i, i + 1) for i in range(n)])
    t1 = time.time()
    bad = sum(1 for g in got if g != "OK")
    got = c.pipeline([("GET", base + i) for i in range(n)])
    t2 = time.time()
    bad += sum(1 for i, g in enumerate(got) if g != str(i + 1))
    if bad:
        failures.append(f"throughput: {bad} bad replies")
    print(f"throughput sanity: {n} SET in {t1-t0:.2f}s ({n/(t1-t0):,.0f} op/s), "
          f"{n} GET in {t2-t1:.2f}s ({n/(t2-t1):,.0f} op/s)")
    c.close()


def test_sweep_to_empty(port, extra, sup):
    """Deletes the whole key space front to back, checking BPF.PRED as it goes,
    then refills the emptied forest.  Every batch takes out the smallest live
    keys, so each round moves the leading partitions' begins and in the end
    empties every partition.  Runs against a server of its own, both
    because it destroys the initial data and because it wants a small one."""
    if not {"del", "pred", "set", "get"} <= sup:
        return
    nr, stride, chunk = 4000, 4, 97
    proc = start_server(port, "--init-nr", str(nr), "--init-stride", str(stride), *extra)
    try:
        keys = [(i + 1) * stride for i in range(nr)]
        alive = set(keys)
        c = Client(port)
        for start in range(0, nr, chunk):
            batch = keys[start:start + chunk]
            got = c.pipeline([("DEL", k) for k in batch])
            check(f"sweep del {start}", got, [1] * len(batch))
            alive.difference_update(batch)
            # just past the dead prefix, well inside the live remainder, and
            # past the last key
            probes = [batch[-1] + 1, keys[min(nr - 1, start + chunk + 500)], keys[-1] + 1]
            got = c.pipeline([("BPF.PRED", p) for p in probes])
            want = []
            for p in probes:
                live = [k for k in alive if k < p]
                want.append([str(max(live))] * 2 if live else None)
            check(f"sweep pred {start}", got, want)
        check("sweep pred on empty", c.cmd("BPF.PRED", keys[-1] + 1), None)
        check("sweep get on empty", c.cmd("GET", keys[0]), None)
        check("sweep refill", c.cmd("SET", 12345, 999), "OK")
        check("sweep get refilled", c.cmd("GET", 12345), "999")
        check("sweep pred refilled", c.cmd("BPF.PRED", 12346), ["12345", "999"])
        c.close()
    finally:
        proc.kill()


def run_all(port, extra, label):
    print(f"=== server {label} ===")
    proc = start_server(port, *extra)
    try:
        c0 = Client(port)
        sup = probe_support(c0)
        c0.close()
        print("supported ops:", ",".join(sorted(sup)))
        model = fresh_model()
        test_basic(port, sup)
        # test_basic consumed some keys of the initial model
        for k in (9, 10):
            model.pop(k, None)
        model[12] = 12
        test_pipeline_serial(port, model, sup)
        test_multi_conn(port)
        test_write_collision(port, sup)
        test_deleted_run_pred(port, sup)
        test_throughput(port)
        test_sweep_to_empty(port + 50, extra, sup)
        c = Client(port)
        c.send("SHUTDOWN")
        time.sleep(0.2)
        c.close()
        proc.wait(timeout=30)
        check("clean shutdown", proc.returncode, 0)
    finally:
        if proc.poll() is None:
            proc.kill()
    tail = proc.stdout.read()
    if tail.strip():
        print("server output tail:", tail.strip()[-500:])


def main():
    run_all(6399, [], "default epoch cap")
    run_all(6400, ["--batch-size", "700"], "small batch size (700)")
    print()
    if failures:
        print(f"{len(failures)} FAILURES")
        for f in failures[:20]:
            print(" -", f)
        sys.exit(1)
    print("ALL TESTS PASSED")


if __name__ == "__main__":
    main()
