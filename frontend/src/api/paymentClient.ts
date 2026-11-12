export type PaymentFlow = "p2p" | "merchant";

export interface PaymentRequest {
  sender_id: string;
  receiver_id: string;
  amount: string;
}

export interface PaymentResult {
  kind: "completed" | "processing";
  payment_id: string;
  status: string;
  amount: string;
  sender_id: string;
  receiver_id: string;
  replayed?: boolean;
}

export interface PaymentStreamEvent {
  payment_id: string;
  sender_id: string;
  receiver_id: string;
  amount: string;
  request_id: string;
  flow: string;
}

export interface AccountBalance {
  account_id: string;
  available_balance: string;
  ledger_balance: string;
  currency: string;
}

export const wsPreBuffer = new Map<string, PaymentStreamEvent[]>();

const subscribers = new Map<string, Set<(event: PaymentStreamEvent) => void>>();
let activeSocket: WebSocket | undefined;
let socketCloseTimer: number | undefined;
let socketReconnectTimer: number | undefined;

export class PaymentApiError extends Error {
  constructor(
    message: string,
    readonly code: string,
    readonly status: number,
    readonly retryable: boolean,
  ) {
    super(message);
    this.name = "PaymentApiError";
  }
}

function streamUrl(): string {
  const url = new URL("/v2/stream", window.location.href);
  url.protocol = url.protocol === "https:" ? "wss:" : "ws:";
  return url.toString();
}

function dispatchStreamEvent(event: PaymentStreamEvent): void {
  const accountIds = new Set([event.sender_id, event.receiver_id]);
  for (const accountId of accountIds) {
    const listeners = subscribers.get(accountId);
    if (!listeners?.size) {
      const buffer = wsPreBuffer.get(accountId) ?? [];
      buffer.push(event);
      wsPreBuffer.set(accountId, buffer.slice(-100));
      continue;
    }
    for (const listener of listeners) listener(event);
  }
}

export function connectPaymentStream(
  accountId: string,
  onEvent: (event: PaymentStreamEvent) => void,
  onError?: () => void,
  onOpen?: () => void,
): () => void {
  const ensureSocket = (): void => {
    if (
      activeSocket &&
      activeSocket.readyState !== WebSocket.CLOSED &&
      activeSocket.readyState !== WebSocket.CLOSING
    ) {
      return;
    }
    const socket = new WebSocket(streamUrl());
    activeSocket = socket;
    socket.onopen = () => onOpen?.();
    socket.onmessage = (message: MessageEvent<string>) => {
      try {
        dispatchStreamEvent(JSON.parse(message.data) as PaymentStreamEvent);
      } catch {
        onError?.();
      }
    };
    socket.onerror = () => onError?.();
    socket.onclose = () => {
      if (activeSocket === socket) activeSocket = undefined;
      if (subscribers.size > 0 && socketReconnectTimer === undefined) {
        socketReconnectTimer = window.setTimeout(() => {
          socketReconnectTimer = undefined;
          if (subscribers.size > 0) ensureSocket();
        }, 1_000);
      }
    };
  };

  const listeners = subscribers.get(accountId) ?? new Set();
  listeners.add(onEvent);
  subscribers.set(accountId, listeners);

  const buffered = wsPreBuffer.get(accountId) ?? [];
  wsPreBuffer.delete(accountId);
  for (const event of buffered) onEvent(event);

  if (socketCloseTimer !== undefined) {
    window.clearTimeout(socketCloseTimer);
    socketCloseTimer = undefined;
  }
  ensureSocket();

  return () => {
    const activeListeners = subscribers.get(accountId);
    activeListeners?.delete(onEvent);
    if (activeListeners?.size === 0) subscribers.delete(accountId);
    if (subscribers.size === 0 && activeSocket && socketCloseTimer === undefined) {
      socketCloseTimer = window.setTimeout(() => {
        socketCloseTimer = undefined;
        if (subscribers.size === 0 && activeSocket) {
          activeSocket.close();
          activeSocket = undefined;
        }
      }, 50);
    }
  };
}

async function decodeResponse<T>(response: Response): Promise<T> {
  const body: unknown = await response.json();
  if (!response.ok) {
    const problem = body as {
      code?: string;
      detail?: string;
      retryable?: boolean;
    };
    throw new PaymentApiError(
      problem.detail ?? "The payment request failed",
      problem.code ?? "HTTP_ERROR",
      response.status,
      problem.retryable ?? response.status >= 500,
    );
  }
  return body as T;
}

export async function submitPayment(
  payment: PaymentRequest,
  flow: PaymentFlow,
  idempotencyKey: string,
  timeoutMs = 5_000,
): Promise<PaymentResult> {
  const fetchPromise = fetch(`/v2/payments/${flow}`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "Idempotency-Key": idempotencyKey,
    },
    body: JSON.stringify(payment),
  });
  let timeoutHandle: number | undefined;
  const timeoutPromise = new Promise<never>((_, reject) => {
    timeoutHandle = window.setTimeout(
      () => reject(new DOMException("Payment response timed out", "TimeoutError")),
      timeoutMs,
    );
  });
  try {
    const response = await Promise.race([fetchPromise, timeoutPromise]);
    const result = await decodeResponse<Omit<PaymentResult, "kind">>(response);
    return {
      ...result,
      kind: response.status === 202 ? "processing" : "completed",
    };
  } catch (error) {
    if (error instanceof DOMException && error.name === "TimeoutError") {
      void fetchPromise.catch(() => undefined);
    }
    throw error;
  } finally {
    if (timeoutHandle !== undefined) window.clearTimeout(timeoutHandle);
  }
}

export async function cancelIdempotencyKeyOnServer(
  idempotencyKey: string,
  accountId: string,
): Promise<void> {
  const response = await fetch(
    `/v2/payments/idempotency/${encodeURIComponent(idempotencyKey)}/cancel`,
    {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ account_id: accountId }),
    },
  );
  await decodeResponse<{ state: string }>(response);
}

export async function getAccountBalance(accountId: string): Promise<AccountBalance> {
  const response = await fetch(`/v2/accounts/${encodeURIComponent(accountId)}/balance`);
  return decodeResponse<AccountBalance>(response);
}
