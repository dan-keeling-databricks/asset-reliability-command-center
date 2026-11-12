import assert from "node:assert/strict";
import test from "node:test";
import { IncidentRefreshCoordinator } from "../src/incidentRefreshCoordinator.ts";

type Deferred<T> = {
  promise: Promise<T>;
  resolve: (value: T) => void;
  reject: (error: unknown) => void;
};

function deferred<T>(): Deferred<T> {
  let resolve!: (value: T) => void;
  let reject!: (error: unknown) => void;
  const promise = new Promise<T>((resolvePromise, rejectPromise) => {
    resolve = resolvePromise;
    reject = rejectPromise;
  });
  return { promise, resolve, reject };
}

class FakeClock {
  now = 0;
  private nextId = 1;
  private timers = new Map<number, { at: number; callback: () => void }>();

  schedule = (callback: () => void, delayMs: number) => {
    const id = this.nextId++;
    this.timers.set(id, { at: this.now + delayMs, callback });
    return id;
  };

  cancel = (id: number) => {
    this.timers.delete(id);
  };

  advance(milliseconds: number): void {
    const target = this.now + milliseconds;
    while (true) {
      const due = [...this.timers.entries()]
        .filter(([, timer]) => timer.at <= target)
        .sort((left, right) => left[1].at - right[1].at)[0];
      if (!due) break;
      this.timers.delete(due[0]);
      this.now = due[1].at;
      due[1].callback();
    }
    this.now = target;
  }
}

async function flushPromises(): Promise<void> {
  await Promise.resolve();
  await Promise.resolve();
}

function harness() {
  const clock = new FakeClock();
  const requests: Array<{ incidentId: string; result: Deferred<string> }> = [];
  const results: string[] = [];
  const errors: unknown[] = [];
  const coordinator = new IncidentRefreshCoordinator<string>({
    intervalMs: 1000,
    load: (incidentId, signal) => {
      const result = deferred<string>();
      signal.addEventListener(
        "abort",
        () => result.reject(new DOMException("Aborted", "AbortError")),
        { once: true }
      );
      requests.push({ incidentId, result });
      return result.promise;
    },
    onResult: (value) => results.push(value),
    onError: (error) => errors.push(error),
    now: () => clock.now,
    schedule: clock.schedule,
    cancel: clock.cancel
  });
  return { clock, coordinator, requests, results, errors };
}

test("coalesces revision bursts without overlapping requests", async () => {
  const { clock, coordinator, requests, results } = harness();
  coordinator.select("A");
  coordinator.request(true);
  clock.advance(0);
  assert.equal(requests.length, 1);

  coordinator.request();
  coordinator.request();
  coordinator.request();
  assert.equal(requests.length, 1);
  requests[0].result.resolve("A-1");
  await flushPromises();
  clock.advance(999);
  assert.equal(requests.length, 1);
  clock.advance(1);
  assert.equal(requests.length, 2);
  assert.deepEqual(results, ["A-1"]);
});

test("switching selection drains the new incident and ignores the old response", async () => {
  const { clock, coordinator, requests, results } = harness();
  coordinator.select("A");
  coordinator.request(true);
  clock.advance(0);
  coordinator.select("B");
  coordinator.request(true);
  requests[0].result.resolve("stale-A");
  await flushPromises();
  clock.advance(1000);
  assert.deepEqual(requests.map((request) => request.incidentId), ["A", "B"]);
  requests[1].result.resolve("fresh-B");
  await flushPromises();
  assert.deepEqual(results, ["fresh-B"]);
});

test("close and reopen of the same incident rejects the old generation", async () => {
  const { clock, coordinator, requests, results } = harness();
  coordinator.select("A");
  coordinator.request(true);
  clock.advance(0);
  coordinator.clear();
  coordinator.select("A");
  coordinator.request(true);
  requests[0].result.resolve("old-generation");
  await flushPromises();
  clock.advance(1000);
  requests[1].result.resolve("new-generation");
  await flushPromises();
  assert.deepEqual(results, ["new-generation"]);
});

test("a failed request still drains one queued refresh", async () => {
  const { clock, coordinator, requests, errors } = harness();
  coordinator.select("A");
  coordinator.request(true);
  clock.advance(0);
  coordinator.request();
  requests[0].result.reject(new Error("transient"));
  await flushPromises();
  clock.advance(1000);
  assert.equal(requests.length, 2);
  assert.equal(errors.length, 1);
});

test("opening an incident performs only its explicit initial request", async () => {
  const { clock, coordinator, requests } = harness();
  coordinator.select("A");
  coordinator.request(true);
  clock.advance(0);
  requests[0].result.resolve("A");
  await flushPromises();
  clock.advance(10_000);
  assert.equal(requests.length, 1);
});

test("a stalled stale selection is aborted so the new selection can load", async () => {
  const { clock, coordinator, requests, results } = harness();
  coordinator.select("A");
  coordinator.request(true);
  clock.advance(0);
  coordinator.select("B");
  coordinator.request(true);
  await flushPromises();
  clock.advance(1000);
  assert.deepEqual(requests.map((request) => request.incidentId), ["A", "B"]);
  requests[1].result.resolve("fresh-B");
  await flushPromises();
  assert.deepEqual(results, ["fresh-B"]);
});
