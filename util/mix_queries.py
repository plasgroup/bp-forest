import random
import math
import bisect


class InputStream:
    __slots__ = ("stream", "buffer", "start")

    def __init__(self, stream):
        self.stream = stream
        self.buffer = bytearray()
        self.start = 0

    def ensure_available(self, chunk_size, buffer_size):
        buf = self.buffer
        start = self.start
        available = len(buf) - start

        while available < chunk_size:
            chunk = self.stream.read(buffer_size)
            if not chunk:
                return False
            buf.extend(chunk)
            available = len(buf) - start

        return True

    def consume(self, chunk_size, buffer_size):
        if not self.ensure_available(chunk_size, buffer_size):
            return None

        buf = self.buffer
        start = self.start
        end = start + chunk_size

        data = memoryview(buf)[start:end]
        self.start = end

        # memmove最小化
        if self.start > buffer_size:
            del buf[:self.start]
            self.start = 0

        return data


class OutputStream:
    __slots__ = ("stream",)

    def __init__(self, stream):
        self.stream = stream

    def write(self, data):
        self.stream.write(data)


class WeightedIndex:
    __slots__ = ("cumulative",)

    def __init__(self, weights):
        if len(weights) == 0:
            raise ValueError("weights must not be empty")

        for i, w in enumerate(weights):
            if not math.isfinite(w) or w <= 0:
                raise ValueError(f"weights[{i}] must be finite and > 0")

        total = float(sum(weights))
        if total <= 0:
            raise ValueError("sum of weights must be positive")

        acc = 0.0
        cumulative = []
        for w in weights:
            acc += w / total
            cumulative.append(acc)

        cumulative[-1] = 1.0
        self.cumulative = cumulative

    def select(self, r):
        return bisect.bisect_left(self.cumulative, r)


class RandomGenerator:
    __slots__ = ("rng",)

    def __init__(self, seed):
        self.rng = random.Random(seed)

    def random_block(self, n):
        return [self.rng.random() for _ in range(n)]


def mix_streams(
    inputs,
    weighted_index,
    rng,
    output,
    output_length,
    chunk_size,
    buffer_size=8 * 1024 * 1024,
    rng_block_size=100_000,
):
    #===== input check =====#

    if output_length <= 0:
        raise ValueError("output_length must be positive")

    if chunk_size <= 0:
        raise ValueError("chunk_size must be positive")

    if chunk_size > buffer_size:
        raise ValueError("chunk_size must not exceed buffer_size")

    if output_length % chunk_size != 0:
        raise ValueError("output_length must be divisible by chunk_size")

    if len(inputs) == 0:
        raise ValueError("inputs must not be empty")

    if len(inputs) != len(weighted_index.cumulative):
        raise ValueError("inputs and weights size mismatch")

    if rng_block_size <= 0:
        raise ValueError("rng_block_size must be positive")

    #===== preparation =====#

    iterations = output_length // chunk_size

    write = output.write
    select = weighted_index.select
    random_block = rng.random_block

    #===== generation =====#

    remaining = iterations

    while remaining > 0:
        block = min(rng_block_size, remaining)
        randoms = random_block(block)

        for r in randoms:
            idx = select(r)
            chunk = inputs[idx].consume(chunk_size, buffer_size)
            if chunk is None:
                raise RuntimeError(f"Input stream {idx} reached EOF")
            write(chunk)

        remaining -= block


def main(seed):
    inputs = [
        InputStream(open("a.bin", "rb")),
        InputStream(open("b.bin", "rb")),
    ]

    weights = WeightedIndex([1, 2])
    rng = RandomGenerator(seed)
    output = OutputStream(open("out.bin", "wb"))

    mix_streams(inputs, weights, rng, output, 1_000_000, 24)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-r", "--rand_seed", type=int, default=1_000_000)
    main(parser.rand_seed)
