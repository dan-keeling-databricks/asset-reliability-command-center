type Selection = {
  id: string;
  generation: number;
};

type IncidentRefreshCoordinatorOptions<T> = {
  intervalMs: number;
  load: (incidentId: string, signal: AbortSignal) => Promise<T>;
  onResult: (value: T) => void;
  onError: (error: unknown) => void;
  now?: () => number;
  schedule?: (callback: () => void, delayMs: number) => number;
  cancel?: (timer: number) => void;
};

/**
 * Serialises incident reads and coalesces high-frequency SSE revisions.
 *
 * A generation distinguishes close/reopen and A-to-B selection changes, so a response from an
 * older selection can never overwrite the current panel. Revisions received during a request are
 * represented by one dirty bit and drained once the minimum refresh interval has elapsed.
 */
export class IncidentRefreshCoordinator<T> {
  private readonly intervalMs: number;
  private readonly load: (incidentId: string, signal: AbortSignal) => Promise<T>;
  private readonly onResult: (value: T) => void;
  private readonly onError: (error: unknown) => void;
  private readonly now: () => number;
  private readonly schedule: (callback: () => void, delayMs: number) => number;
  private readonly cancel: (timer: number) => void;
  private selection: Selection | null = null;
  private generation = 0;
  private dirty = false;
  private timer: number | null = null;
  private inFlight: Promise<void> | null = null;
  private inFlightAbortController: AbortController | null = null;
  private lastStartedAt = Number.NEGATIVE_INFINITY;
  private disposed = false;

  constructor(options: IncidentRefreshCoordinatorOptions<T>) {
    this.intervalMs = options.intervalMs;
    this.load = options.load;
    this.onResult = options.onResult;
    this.onError = options.onError;
    this.now = options.now ?? Date.now;
    this.schedule = options.schedule ?? ((callback, delayMs) => window.setTimeout(callback, delayMs));
    this.cancel = options.cancel ?? ((timer) => window.clearTimeout(timer));
  }

  select(incidentId: string): void {
    this.generation += 1;
    this.selection = { id: incidentId, generation: this.generation };
    this.dirty = false;
    this.cancelTimer();
    this.inFlightAbortController?.abort();
  }

  clear(): void {
    this.generation += 1;
    this.selection = null;
    this.dirty = false;
    this.cancelTimer();
    this.inFlightAbortController?.abort();
  }

  request(immediate = false): void {
    if (this.disposed || !this.selection) return;
    this.dirty = true;
    if (this.inFlight) return;
    if (this.timer !== null) {
      if (!immediate) return;
      this.cancelTimer();
    }
    const elapsed = this.now() - this.lastStartedAt;
    const delay = immediate ? 0 : Math.max(0, this.intervalMs - elapsed);
    this.timer = this.schedule(() => this.start(), delay);
  }

  dispose(): void {
    this.disposed = true;
    this.clear();
  }

  private cancelTimer(): void {
    if (this.timer === null) return;
    this.cancel(this.timer);
    this.timer = null;
  }

  private start(): void {
    this.timer = null;
    if (this.disposed || this.inFlight || !this.dirty || !this.selection) return;
    this.dirty = false;
    const requestedSelection = this.selection;
    this.lastStartedAt = this.now();
    const abortController = new AbortController();
    this.inFlightAbortController = abortController;
    const request = this.load(requestedSelection.id, abortController.signal)
      .then((value) => {
        if (this.isCurrent(requestedSelection)) this.onResult(value);
      })
      .catch((error: unknown) => {
        if (this.isCurrent(requestedSelection)) this.onError(error);
      })
      .finally(() => {
        this.inFlight = null;
        this.inFlightAbortController = null;
        if (this.dirty) this.request();
      });
    this.inFlight = request;
  }

  private isCurrent(selection: Selection): boolean {
    return !this.disposed
      && this.selection?.id === selection.id
      && this.selection.generation === selection.generation;
  }
}
