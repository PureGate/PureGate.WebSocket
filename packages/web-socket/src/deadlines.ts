export interface Deadline {
  /** performance.now() when it passes. */
  readonly deadline: number;
}

/**
 * One timer for every call's deadline, set for the nearest. When it fires, every item whose deadline passed expires,
 * and the timer moves to the next.
 */
export class Deadlines<T extends Deadline> {
  readonly #items = new Set<T>();
  readonly #expire: (item: T) => void;
  #timer: ReturnType<typeof setTimeout> | undefined;
  #at = Number.POSITIVE_INFINITY;

  constructor(expire: (item: T) => void) {
    this.#expire = expire;
  }

  add(item: T): void {
    this.#items.add(item);
    if (item.deadline < this.#at) this.#schedule(item.deadline);
  }

  delete(item: T): void {
    this.#items.delete(item);
    if (this.#items.size === 0) this.#stop();
  }

  clear(): void {
    this.#items.clear();
    this.#stop();
  }

  #schedule(at: number): void {
    this.#stop();
    this.#at = at;
    this.#timer = setTimeout(() => this.#fire(), Math.max(0, at - performance.now()));
  }

  #stop(): void {
    if (this.#timer !== undefined) clearTimeout(this.#timer);
    this.#timer = undefined;
    this.#at = Number.POSITIVE_INFINITY;
  }

  #fire(): void {
    this.#timer = undefined;
    this.#at = Number.POSITIVE_INFINITY;
    const now = performance.now();
    let next = Number.POSITIVE_INFINITY;
    for (const item of this.#items) {
      if (item.deadline <= now) {
        this.#items.delete(item);
        this.#expire(item);
      } else if (item.deadline < next) {
        next = item.deadline;
      }
    }

    if (next !== Number.POSITIVE_INFINITY && this.#timer === undefined) this.#schedule(next);
  }
}
