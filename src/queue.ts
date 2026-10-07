/**
 * FIFO JSON storage with byte accounting and original arrival deadlines.
 * Parallel arrays share the same head index to avoid a wrapper object per record.
 */
export class RecordQueue {
  /**
   * Consumed JSON strings are cleared immediately to release their storage.
   */
  private records: Array<string | undefined> = [];

  /**
   * Serialized UTF-8 sizes aligned with the JSON storage.
   */
  private sizes: number[] = [];

  /**
   * Monotonic arrival times aligned with the JSON storage.
   */
  private timestamps: number[] = [];

  /**
   * Index of the next waiting record.
   */
  private head = 0;

  /**
   * UTF-8 bytes retained in waiting records, excluding delimiters.
   */
  private queuedBytes = 0;

  /**
   * Sequence of the oldest waiting record; accepted records are contiguous.
   */
  private firstSequence = 0;

  /**
   * Number of waiting records.
   */
  get length(): number {
    return this.records.length - this.head;
  }

  /**
   * Total serialized size of waiting records.
   */
  get bytes(): number {
    return this.queuedBytes;
  }

  /**
   * Arrival time of the oldest waiting record, or zero when empty.
   */
  get oldestReceivedAt(): number {
    return this.timestamps[this.head] ?? 0;
  }

  /**
   * Sequence of the oldest waiting record, used only while the queue is nonempty.
   */
  get oldestSequence(): number {
    return this.firstSequence;
  }

  /**
   * Retains validated JSON and its size, deadline, and flush sequence.
   */
  enqueue(json: string, bytes: number, receivedAt: number, sequence: number): void {
    if (this.length === 0) this.firstSequence = sequence;
    this.records.push(json);
    this.sizes.push(bytes);
    this.timestamps.push(receivedAt);
    this.queuedBytes += bytes;
  }

  /**
   * Discards the oldest waiting record and immediately releases its storage.
   */
  dropOldest(): void {
    if (this.length === 0) return;
    this.queuedBytes -= this.sizes[this.head] ?? 0;
    this.records[this.head++] = undefined;
    this.firstSequence++;
    this.compact();
  }

  /**
   * Takes a FIFO prefix within both record and complete JSON body limits.
   */
  take(maxRecords: number, maxBytes: number): string[] {
    const batch: string[] = [];
    let bytes = 2;
    while (batch.length < maxRecords && this.length > 0) {
      const size = this.sizes[this.head] ?? 0;
      const nextBytes = bytes + size + (batch.length > 0 ? 1 : 0);
      if (nextBytes > maxBytes) break;
      bytes = nextBytes;
      const json = this.records[this.head];
      if (json !== undefined) batch.push(json);
      this.queuedBytes -= size;
      this.records[this.head++] = undefined;
      this.firstSequence++;
    }
    this.compact();
    return batch;
  }

  /**
   * Reclaims consumed slots while avoiding repeated array-front shifts.
   */
  private compact(): void {
    if (this.head === this.records.length) {
      this.records = [];
      this.sizes = [];
      this.timestamps = [];
      this.head = 0;
    } else if (this.head >= 4096 && this.head * 2 >= this.records.length) {
      this.records = this.records.slice(this.head);
      this.sizes = this.sizes.slice(this.head);
      this.timestamps = this.timestamps.slice(this.head);
      this.head = 0;
    }
  }
}
