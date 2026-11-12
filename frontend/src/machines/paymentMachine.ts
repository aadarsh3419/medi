import { assign, fromPromise, setup } from "xstate";
import {
  cancelIdempotencyKeyOnServer,
  PaymentApiError,
  submitPayment,
  type PaymentFlow,
  type PaymentRequest,
  type PaymentResult,
} from "../api/paymentClient";

interface PaymentContext {
  payment: PaymentRequest;
  flow: PaymentFlow;
  idempotencyKey: string;
  result?: PaymentResult;
  error?: string;
  timeoutMs: number;
  keyStatus: "not-created" | "processing" | "uncertain" | "completed" | "cancelled" | "failed";
}

type PaymentEvent =
  | { type: "UPDATE"; payment: PaymentRequest; flow: PaymentFlow }
  | {
      type: "SUBMIT";
      payment: PaymentRequest;
      flow: PaymentFlow;
      idempotencyKey: string;
      timeoutMs: number;
    }
  | { type: "RETRY" }
  | { type: "CANCEL" };

interface SubmitInput {
  payment: PaymentRequest;
  flow: PaymentFlow;
  idempotencyKey: string;
  timeoutMs: number;
}

interface CancelInput {
  idempotencyKey: string;
  accountId: string;
}

const submitActor = fromPromise<PaymentResult, SubmitInput>(({ input }) =>
  submitPayment(input.payment, input.flow, input.idempotencyKey, input.timeoutMs),
);

const cancelActor = fromPromise<void, CancelInput>(({ input }) =>
  cancelIdempotencyKeyOnServer(input.idempotencyKey, input.accountId),
);

function isTimeout(error: unknown): boolean {
  return error instanceof DOMException && error.name === "TimeoutError";
}

function isPaymentResult(value: unknown): value is PaymentResult {
  return (
    typeof value === "object" &&
    value !== null &&
    "kind" in value &&
    (value.kind === "completed" || value.kind === "processing")
  );
}

function errorText(error: unknown): string {
  if (error instanceof PaymentApiError) return `${error.code}: ${error.message}`;
  if (error instanceof Error) return error.message;
  return "The payment could not be completed";
}

export const paymentMachine = setup({
  types: {
    context: {} as PaymentContext,
    events: {} as PaymentEvent,
  },
  actors: {
    submitPayment: submitActor,
    cancelIdempotencyKey: cancelActor,
  },
  guards: {
    responseIsProcessing: ({ event }) =>
      "output" in event &&
      isPaymentResult(event.output) &&
      event.output.kind === "processing",
    responseTimedOut: ({ event }) =>
      "error" in event && isTimeout(event.error),
  },
  actions: {
    captureSubmission: assign(({ event }) => {
      if (event.type !== "SUBMIT") return {};
      return {
        payment: event.payment,
        flow: event.flow,
        idempotencyKey: event.idempotencyKey,
        timeoutMs: event.timeoutMs,
        error: undefined,
        result: undefined,
        keyStatus: "processing",
      };
    }),
    captureResult: assign(({ event }) => {
      const result =
        "output" in event && isPaymentResult(event.output) ? event.output : undefined;
      return {
        result,
        error: undefined,
        keyStatus: result?.kind === "processing" ? "processing" as const : "completed" as const,
      };
    }),
    captureError: assign(({ event }) => {
      const error = "error" in event ? event.error : undefined;
      return {
        error: errorText(error),
        keyStatus: isTimeout(error) ? "uncertain" as const : "failed" as const,
      };
    }),
    prepareRetry: assign({ timeoutMs: 5_000, error: undefined, keyStatus: "processing" as const }),
    clearForEdit: assign({
      error: undefined,
      result: undefined,
      idempotencyKey: "",
      keyStatus: "not-created" as const,
    }),
  },
}).createMachine({
  id: "payment",
  initial: "editing",
  context: {
    payment: { sender_id: "acc_asha", receiver_id: "acc_ravi", amount: "10.0000" },
    flow: "p2p",
    idempotencyKey: "",
    timeoutMs: 5_000,
    keyStatus: "not-created",
  },
  states: {
    editing: {
      on: {
        UPDATE: {
          actions: assign(({ event }) =>
            event.type === "UPDATE"
              ? {
                  payment: event.payment,
                  flow: event.flow,
                  result: undefined,
                  idempotencyKey: "",
                  keyStatus: "not-created",
                }
              : {},
          ),
        },
        SUBMIT: { target: "submitting", actions: "captureSubmission" },
      },
    },
    submitting: {
      invoke: {
        src: "submitPayment",
        input: ({ context }) => ({
          payment: context.payment,
          flow: context.flow,
          idempotencyKey: context.idempotencyKey,
          timeoutMs: context.timeoutMs,
        }),
        onDone: [
          {
            guard: "responseIsProcessing",
            target: "processing_202",
            actions: "captureResult",
          },
          { target: "completed", actions: "captureResult" },
        ],
        onError: [
          {
            guard: "responseTimedOut",
            target: "uncertain_timeout",
            actions: "captureError",
          },
          { target: "failed", actions: "captureError" },
        ],
      },
    },
    processing_202: {
      on: {
        RETRY: { target: "submitting", actions: "prepareRetry" },
        CANCEL: "cancelling_key",
      },
    },
    uncertain_timeout: {
      on: {
        RETRY: { target: "submitting", actions: "prepareRetry" },
        CANCEL: "cancelling_key",
      },
    },
    cancelling_key: {
      invoke: {
        src: "cancelIdempotencyKey",
        input: ({ context }) => ({
          idempotencyKey: context.idempotencyKey,
          accountId: context.payment.sender_id,
        }),
        onDone: {
          target: "failed",
          actions: assign({
            error: "Idempotency key cancelled on the server",
            keyStatus: "cancelled",
          }),
        },
        onError: {
          target: "uncertain_timeout",
          actions: "captureError",
        },
      },
    },
    completed: {
      on: {
        UPDATE: {
          target: "editing",
          actions: [
            assign(({ event }) =>
              event.type === "UPDATE"
                ? { payment: event.payment, flow: event.flow }
                : {},
            ),
            "clearForEdit",
          ],
        },
      },
    },
    failed: {
      on: {
        UPDATE: {
          target: "editing",
          actions: [
            assign(({ event }) =>
              event.type === "UPDATE"
                ? { payment: event.payment, flow: event.flow }
                : {},
            ),
            "clearForEdit",
          ],
        },
      },
    },
  },
});
