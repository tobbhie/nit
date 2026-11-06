export interface TelegramMessageBody {
  readonly chat_id: string;
  readonly text: string;
  readonly reply_markup?: {
    readonly inline_keyboard: readonly (readonly { text: string; callback_data: string }[])[];
  };
}
export interface TelegramTransport {
  send(body: TelegramMessageBody): Promise<number>;
  acknowledge(callbackId: string, text: string): Promise<void>;
  removeKeyboard(chatId: string, messageId: number): Promise<void>;
  updates(offset: number, signal?: AbortSignal): Promise<readonly unknown[]>;
}
export class TelegramFailure extends Error {
  constructor(
    message: string,
    readonly ambiguous: boolean,
    readonly retryAfterMs: number | null = null
  ) {
    super(message);
  }
}
interface HttpResponse {
  readonly ok: boolean;
  readonly status: number;
  json(): Promise<unknown>;
}
export type TelegramFetch = (
  url: string,
  init: { method: 'POST'; headers: Record<string, string>; body: string; signal: AbortSignal }
) => Promise<HttpResponse>;

/** Fixed Telegram host; neither credentials nor request URLs appear in exceptions. */
export class TelegramBotApi implements TelegramTransport {
  private nextRequestAt = 0;
  private tail: Promise<void> = Promise.resolve();
  constructor(
    private readonly token: string,
    private readonly fetcher: TelegramFetch = fetch
  ) {
    if (!/^\d+:[A-Za-z0-9_-]+$/.test(token)) throw new Error('Invalid Telegram bot token format');
  }
  async send(body: TelegramMessageBody): Promise<number> {
    if (body.text.length === 0 || body.text.length > 4096)
      throw new Error('Telegram message exceeds its text bound');
    for (const row of body.reply_markup?.inline_keyboard ?? [])
      for (const button of row) {
        if (Buffer.byteLength(button.callback_data, 'utf8') > 64)
          throw new Error('Telegram callback exceeds 64 bytes');
      }
    const result = record(await this.call('sendMessage', body));
    if (typeof result?.['message_id'] !== 'number' || !Number.isSafeInteger(result['message_id']))
      throw new TelegramFailure('Telegram returned an invalid message identity', true);
    return result['message_id'];
  }
  async acknowledge(callbackId: string, text: string): Promise<void> {
    await this.call('answerCallbackQuery', {
      callback_query_id: callbackId,
      text: text.slice(0, 180),
    });
  }
  async removeKeyboard(chatId: string, messageId: number): Promise<void> {
    await this.call('editMessageReplyMarkup', {
      chat_id: chatId,
      message_id: messageId,
      reply_markup: { inline_keyboard: [] },
    });
  }
  async updates(offset: number, signal?: AbortSignal): Promise<readonly unknown[]> {
    const result = await this.call(
      'getUpdates',
      { offset, limit: 25, timeout: 25, allowed_updates: ['message', 'callback_query'] },
      signal
    );
    if (!Array.isArray(result))
      throw new TelegramFailure('Telegram returned invalid updates', false);
    return result as unknown[];
  }
  private async call(method: string, body: unknown, external?: AbortSignal): Promise<unknown> {
    const prior = this.tail;
    let release = (): void => undefined;
    this.tail = new Promise<void>((resolve) => {
      release = resolve;
    });
    await prior;
    try {
      const delay = Math.max(0, this.nextRequestAt - Date.now());
      if (delay > 0) await new Promise((resolve) => setTimeout(resolve, delay));
      this.nextRequestAt = Date.now() + 1000;
    } finally {
      release();
    }
    const controller = new AbortController();
    const abort = (): void => controller.abort();
    external?.addEventListener('abort', abort, { once: true });
    if (external?.aborted) controller.abort();
    const timer = setTimeout(abort, 35_000);
    try {
      const response = await this.fetcher(`https://api.telegram.org/bot${this.token}/${method}`, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify(body),
        signal: controller.signal,
      });
      if (response.status >= 500)
        throw new TelegramFailure(`Telegram ${method} server failure`, true);
      const envelope = record(await response.json());
      if (!response.ok || envelope?.['ok'] !== true) {
        const parameters = record(envelope?.['parameters']);
        const retry = parameters?.['retry_after'];
        throw new TelegramFailure(
          `Telegram ${method} rejected the request`,
          false,
          typeof retry === 'number' && Number.isFinite(retry)
            ? Math.min(300_000, Math.max(1000, retry * 1000))
            : null
        );
      }
      return envelope['result'];
    } catch (error) {
      if (error instanceof TelegramFailure) throw error;
      throw new TelegramFailure(`Telegram ${method} transport outcome is unknown`, true);
    } finally {
      clearTimeout(timer);
      external?.removeEventListener('abort', abort);
    }
  }
}
function record(value: unknown): Record<string, unknown> | null {
  return typeof value === 'object' && value !== null && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : null;
}
