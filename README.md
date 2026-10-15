# Lock-Free SPSC Ring Buffer on ESP32 (Dual-Core, Tested)

A single-producer, single-consumer (SPSC) byte ring buffer for the ESP32, written with `std::atomic` acquire/release ordering and tested with the producer on core 0 and the consumer on core 1. The sketch checks that every byte arrives in order, forces the buffer full, counts overflow drops exactly, and compares throughput against a FreeRTOS queue.

## Design

- **Storage:** 512-byte array. The capacity is a power of two, so index wrap-around is a bitwise mask (`index & 511`).
- **Indices:** `head` is written only by the producer and `tail` only by the consumer. Both are free-running 32-bit counters, so the buffer is full when `head - tail >= capacity` and empty when `head == tail`.
- **Memory ordering:** the producer writes the byte, then publishes it with a `release` store to `head`. The consumer reads `head` with `acquire`, reads the byte, then frees the slot with a `release` store to `tail`. There are no locks.
- **Producer modes:** *lossless* (spin until there is room) and *lossy* (drop the byte and count it).

## What the tests check

The consumer verifies that each byte equals an 8-bit sequence counter that increments only for accepted bytes. Each phase also checks that `accepted + dropped = total` and `consumed = accepted`.

| Phase | Setup | Pass condition |
|---|---|---|
| A | 500,000 bytes, lossless, fast consumer | 0 errors, 0 drops, `consumed = 500,000` |
| B | 200,000 bytes, lossless, slow consumer (sleeps 300 µs every 256 bytes) | 0 errors, 0 drops, the producer had to wait on a full buffer |
| C | 200,000 bytes, lossy, slow consumer | 0 errors, drops occurred, `accepted + dropped = 200,000` |
| D | 500,000 bytes through a 512-entry FreeRTOS queue (baseline) | 0 errors |

## Results

Run on an ESP32 WROOM-32 dev board (ESP32-D0WD-V3) with Arduino-ESP32 core 3.3.12:

```text
[SUMMARY] 4/4 PASS
[RESULT] Ring buffer 3035 KB/s vs FreeRTOS queue 427 KB/s (7.1x)
```

| Phase | Accepted | Dropped | Consumed | Errors | Failed pushes (full waits) | Peak occupancy | Time |
|---|---|---|---|---|---|---|---|
| A | 500,000 | 0 | 500,000 | 0 | 0 | 137 / 512 | 160 ms |
| B | 200,000 | 0 | 200,000 | 0 | 1,405,278 | 512 / 512 | 289 ms |
| C | 39,424 | 160,576 | 39,424 | 0 | 0 | 512 / 512 | 57 ms |
| D (queue) | n/a | n/a | 500,000 | 0 | n/a | n/a | 1,143 ms |

A second run gave the same ring-buffer figure (3035 KB/s) and 426 KB/s for the queue. In total, 739,424 bytes passed through the ring buffer across phases A–C in order with zero errors.

**Read the 7.1x figure with care.** It compares byte-at-a-time handoff between two cores. The ring-buffer consumer busy-polls and keeps its core fully busy, whereas the queue consumer blocks and is scheduled by FreeRTOS. This is not a general speed claim.

## Limitations

- SPSC only. Two producers or two consumers are not supported.
- Both sides poll. There is no blocking wait or task notification.
- Byte-granular operations only. There is no bulk read/write API.
- The producer in the tests is a task, not an interrupt handler.
- The "peak occupancy" is measured on the producer side and is an upper bound.
- Phase C throughput counts dropped bytes as offered load, so it is not meaningful as a speed figure.
- One hardware configuration and one buffer size. Passing these runs shows no errors in these tests, not that every interleaving is correct.

## Build and run

1. Install the Arduino IDE and the **esp32 by Espressif Systems** board package.
2. Open `RingBuffer_Test.ino`, select **ESP32 Dev Module** and your COM port, then upload.
3. Open the Serial Monitor at **115200** baud. The tests take about 15 seconds. Send `r` to run them again.

No wiring is needed. The build uses 272,080 bytes of flash (20%) and 22,684 bytes of RAM (6%).

```text
└── RingBuffer_Test/
    └── RingBuffer_Test.ino
```

## Roadmap

- [ ] Test with an interrupt-driven producer (hardware timer or UART)
- [ ] Add bulk read/write and a blocking wait using task notifications
- [ ] Compare against a mutex-protected buffer
- [ ] Measure CPU load per core during each phase
