/*
 * Lock-Free SPSC Ring Buffer - integrity, overflow and throughput tests
 * ESP32, Arduino IDE. Producer runs on core 0, consumer on core 1.
 *
 *  A: lossless, fast consumer      -> data integrity + throughput
 *  B: lossless, slow consumer      -> buffer fills, producer must wait
 *  C: lossy,    slow consumer      -> overflow path, exact drop accounting
 *  D: FreeRTOS queue baseline      -> same job with a queue, for comparison
 *
 * Send 'r' in the Serial Monitor to run again.
 */

#include <Arduino.h>
#include <atomic>
#include "freertos/FreeRTOS.h"
#include "freertos/task.h"
#include "freertos/semphr.h"
#include "freertos/queue.h"

#define RB_CAPACITY   512            // must be a power of two
#define RB_MASK       (RB_CAPACITY - 1)
#define N_THROUGHPUT  500000UL
#define N_STRESS      200000UL
#define SLOW_EVERY    256            // slow consumer sleeps after this many bytes
#define SLOW_US       300

typedef struct {
    uint8_t storage[RB_CAPACITY];
    std::atomic<uint32_t> head;      // written by the producer only
    std::atomic<uint32_t> tail;      // written by the consumer only
    uint32_t peak;                   // producer only
} ring_t;

typedef struct {
    const char *name;
    uint32_t total;
    bool lossless;
    bool slow_consumer;
    bool expect_full_waits;
    bool expect_drops;
} phase_cfg_t;

static ring_t g_rb;
static std::atomic<bool> g_prod_finished;

static volatile uint32_t g_total, g_errors, g_consumed, g_accepted, g_dropped, g_full_waits;
static volatile bool g_lossless, g_slow;

static SemaphoreHandle_t g_prodDone, g_consDone;
static QueueHandle_t g_q;

/* ------------------------------ Ring buffer ------------------------------- */

static bool Rb_Push(ring_t *rb, uint8_t b) {
    uint32_t h = rb->head.load(std::memory_order_relaxed);
    uint32_t t = rb->tail.load(std::memory_order_acquire);
    if ((h - t) >= RB_CAPACITY) return false;                // full
    rb->storage[h & RB_MASK] = b;
    rb->head.store(h + 1, std::memory_order_release);        // publish the byte
    uint32_t occ = h + 1 - t;                                // upper bound on occupancy
    if (occ > rb->peak) rb->peak = occ;
    return true;
}

static bool Rb_Pop(ring_t *rb, uint8_t *out) {
    uint32_t t = rb->tail.load(std::memory_order_relaxed);
    uint32_t h = rb->head.load(std::memory_order_acquire);
    if (h == t) return false;                                // empty
    *out = rb->storage[t & RB_MASK];
    rb->tail.store(t + 1, std::memory_order_release);        // free the slot
    return true;
}

/* ------------------------------ Ring tasks -------------------------------- */

static void RingProducerTask(void *arg) {
    uint32_t accepted = 0, dropped = 0, waits = 0;
    for (uint32_t i = 0; i < g_total; i++) {
        uint8_t b = (uint8_t)accepted;               // sequence counts accepted bytes only
        if (g_lossless) {
            while (!Rb_Push(&g_rb, b)) waits++;      // retry until there is room
            accepted++;
        } else {
            if (Rb_Push(&g_rb, b)) accepted++; else dropped++;
        }
    }
    g_accepted = accepted;
    g_dropped = dropped;
    g_full_waits = waits;
    g_prod_finished.store(true, std::memory_order_release);
    xSemaphoreGive(g_prodDone);
    vTaskDelete(NULL);
}

static void RingConsumerTask(void *arg) {
    uint32_t consumed = 0, errors = 0;
    for (;;) {
        uint8_t b;
        bool got = Rb_Pop(&g_rb, &b);
        if (!got) {
            if (!g_prod_finished.load(std::memory_order_acquire)) continue;
            got = Rb_Pop(&g_rb, &b);                 // final drain after the producer is done
            if (!got) break;
        }
        if (b != (uint8_t)consumed) errors++;        // must arrive in order, unchanged
        consumed++;
        if (g_slow && (consumed % SLOW_EVERY) == 0) delayMicroseconds(SLOW_US);
    }
    g_consumed = consumed;
    g_errors = errors;
    xSemaphoreGive(g_consDone);
    vTaskDelete(NULL);
}

/* ----------------------------- Queue baseline ----------------------------- */

static void QueueProducerTask(void *arg) {
    for (uint32_t i = 0; i < g_total; i++) {
        uint8_t b = (uint8_t)i;
        xQueueSend(g_q, &b, portMAX_DELAY);
    }
    xSemaphoreGive(g_prodDone);
    vTaskDelete(NULL);
}

static void QueueConsumerTask(void *arg) {
    uint32_t consumed = 0, errors = 0;
    uint8_t b;
    for (uint32_t i = 0; i < g_total; i++) {
        xQueueReceive(g_q, &b, portMAX_DELAY);
        if (b != (uint8_t)i) errors++;
        consumed++;
    }
    g_consumed = consumed;
    g_errors = errors;
    xSemaphoreGive(g_consDone);
    vTaskDelete(NULL);
}

/* ------------------------------ Test runner ------------------------------- */

static const phase_cfg_t phases[] = {
    {"A: lossless, fast consumer",               N_THROUGHPUT, true,  false, false, false},
    {"B: lossless, slow consumer (buffer fills)", N_STRESS,     true,  true,  true,  false},
    {"C: lossy, slow consumer (overflow path)",   N_STRESS,     false, true,  false, true},
};

static uint32_t Kbps(uint32_t bytes, uint32_t dt_us) {
    if (dt_us == 0) dt_us = 1;
    return (uint32_t)(((uint64_t)bytes * 1000000ULL) / dt_us / 1024ULL);
}

static bool Run_Ring_Phase(const phase_cfg_t &p, uint32_t *tp_kbps) {
    Serial.printf("\n[PHASE %s] %u bytes\n", p.name, (unsigned)p.total);

    g_rb.head.store(0);
    g_rb.tail.store(0);
    g_rb.peak = 0;
    memset(g_rb.storage, 0, sizeof(g_rb.storage));
    g_total = p.total; g_lossless = p.lossless; g_slow = p.slow_consumer;
    g_errors = 0; g_consumed = 0; g_accepted = 0; g_dropped = 0; g_full_waits = 0;
    g_prod_finished.store(false);
    xSemaphoreTake(g_prodDone, 0);
    xSemaphoreTake(g_consDone, 0);

    uint32_t t0 = micros();
    // Producer first, both at priority 1, so the Arduino loop task (core 1,
    // priority 1) is never starved before it reaches xSemaphoreTake below.
    xTaskCreatePinnedToCore(RingProducerTask, "rbProd", 4096, NULL, 1, NULL, 0);
    xTaskCreatePinnedToCore(RingConsumerTask, "rbCons", 4096, NULL, 1, NULL, 1);
    bool done = (xSemaphoreTake(g_prodDone, pdMS_TO_TICKS(30000)) == pdTRUE) &&
                (xSemaphoreTake(g_consDone, pdMS_TO_TICKS(30000)) == pdTRUE);
    uint32_t dt = micros() - t0;

    *tp_kbps = Kbps(p.total, dt);
    bool ok = done && g_errors == 0 && g_consumed == g_accepted &&
              (g_accepted + g_dropped) == p.total;
    if (p.lossless)         ok = ok && g_dropped == 0 && g_accepted == p.total;
    if (p.expect_full_waits) ok = ok && g_full_waits > 0;
    if (p.expect_drops)      ok = ok && g_dropped > 0;

    Serial.printf("   accepted=%u dropped=%u consumed=%u errors=%u full_waits=%u peak=%u/%u\n",
                  (unsigned)g_accepted, (unsigned)g_dropped, (unsigned)g_consumed,
                  (unsigned)g_errors, (unsigned)g_full_waits,
                  (unsigned)g_rb.peak, (unsigned)RB_CAPACITY);
    Serial.printf("   time=%u ms  throughput=%u KB/s\n", (unsigned)(dt / 1000), (unsigned)*tp_kbps);
    Serial.println(ok ? "   -> PASS" : "   -> FAIL");
    return ok;
}

static bool Run_Queue_Phase(uint32_t *tp_kbps) {
    Serial.printf("\n[PHASE D: FreeRTOS queue baseline] %u bytes (may take a few seconds)\n",
                  (unsigned)N_THROUGHPUT);
    if (g_q) vQueueDelete(g_q);
    g_q = xQueueCreate(RB_CAPACITY, sizeof(uint8_t));
    g_total = N_THROUGHPUT; g_errors = 0; g_consumed = 0;
    xSemaphoreTake(g_prodDone, 0);
    xSemaphoreTake(g_consDone, 0);

    uint32_t t0 = micros();
    xTaskCreatePinnedToCore(QueueConsumerTask, "qCons", 4096, NULL, 2, NULL, 1);
    xTaskCreatePinnedToCore(QueueProducerTask, "qProd", 4096, NULL, 2, NULL, 0);
    bool done = (xSemaphoreTake(g_prodDone, pdMS_TO_TICKS(60000)) == pdTRUE) &&
                (xSemaphoreTake(g_consDone, pdMS_TO_TICKS(60000)) == pdTRUE);
    uint32_t dt = micros() - t0;

    *tp_kbps = Kbps(N_THROUGHPUT, dt);
    bool ok = done && g_errors == 0 && g_consumed == N_THROUGHPUT;
    Serial.printf("   consumed=%u errors=%u\n", (unsigned)g_consumed, (unsigned)g_errors);
    Serial.printf("   time=%u ms  throughput=%u KB/s\n", (unsigned)(dt / 1000), (unsigned)*tp_kbps);
    Serial.println(ok ? "   -> PASS" : "   -> FAIL");
    return ok;
}

static void Run_All_Tests(void) {
    Serial.println("\n--- Running ring buffer tests (2 cores) ---");
    int pass = 0;
    uint32_t tpA = 0, tpD = 0, tmp = 0;

    if (Run_Ring_Phase(phases[0], &tpA)) pass++;
    vTaskDelay(pdMS_TO_TICKS(50));
    if (Run_Ring_Phase(phases[1], &tmp)) pass++;
    vTaskDelay(pdMS_TO_TICKS(50));
    if (Run_Ring_Phase(phases[2], &tmp)) pass++;
    vTaskDelay(pdMS_TO_TICKS(50));
    if (Run_Queue_Phase(&tpD)) pass++;

    Serial.printf("\n[SUMMARY] %d/4 PASS\n", pass);
    if (tpD > 0) {
        Serial.printf("[RESULT] Ring buffer %u KB/s vs FreeRTOS queue %u KB/s (%.1fx)\n",
                      (unsigned)tpA, (unsigned)tpD, (double)tpA / (double)tpD);
    }
    Serial.println("Send 'r' to run again.");
}

/* -------------------------------- Arduino --------------------------------- */

void setup() {
    Serial.begin(115200);
    delay(1000);

    Serial.println("\n=======================================================");
    Serial.println("  Lock-Free SPSC Ring Buffer - Integrity & Throughput");
    Serial.println("  Producer: core 0 | Consumer: core 1 | Capacity: 512 B");
    Serial.println("=======================================================");

    g_prodDone = xSemaphoreCreateBinary();
    g_consDone = xSemaphoreCreateBinary();
    g_q = NULL;

    Run_All_Tests();
}

void loop() {
    if (Serial.available()) {
        char c = Serial.read();
        if (c == 'r' || c == 'R') Run_All_Tests();
    }
    delay(20);
}