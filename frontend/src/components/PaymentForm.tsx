import { useEffect, useState } from "react";
import { useMachine } from "@xstate/react";
import {
  connectPaymentStream,
  getAccountBalance,
  type PaymentStreamEvent,
} from "../api/paymentClient";
import { paymentMachine } from "../machines/paymentMachine";

export function PaymentForm() {
  const [state, send] = useMachine(paymentMachine);
  const [streamEvent, setStreamEvent] = useState<PaymentStreamEvent>();
  const [streamState, setStreamState] = useState("Connecting");
  const [availableBalance, setAvailableBalance] = useState<string>();
  const [balanceError, setBalanceError] = useState<string>();
  const payment = state.context.payment;
  const flow = state.context.flow;

  useEffect(() => {
    setStreamState("Connecting");
    let active = true;
    const refreshBalance = () => {
      void getAccountBalance(payment.sender_id)
        .then((balance) => {
          if (active) {
            setAvailableBalance(balance.available_balance);
            setBalanceError(undefined);
          }
        })
        .catch(() => {
          if (active) setBalanceError("Balance refresh unavailable");
        });
    };
    refreshBalance();
    const disconnect = connectPaymentStream(
      payment.sender_id,
      (event) => {
        setStreamEvent(event);
        setStreamState("Live");
        refreshBalance();
      },
      () => setStreamState("Reconnecting"),
      () => setStreamState("Live"),
    );
    return () => {
      active = false;
      disconnect();
    };
  }, [payment.sender_id]);

  useEffect(() => {
    let active = true;
    if (state.context.result?.payment_id) {
      void getAccountBalance(payment.sender_id).then(
        (balance) => {
          if (active) setAvailableBalance(balance.available_balance);
        },
        () => {
          if (active) setBalanceError("Balance refresh unavailable");
        },
      );
    }
    return () => {
      active = false;
    };
  }, [payment.sender_id, state.context.result?.payment_id]);

  const updatePayment = (
    changed: Partial<typeof payment>,
    nextFlow = flow,
  ) => {
    send({
      type: "UPDATE",
      payment: { ...payment, ...changed },
      flow: nextFlow,
    });
  };

  const submit = (timeoutMs: number) => {
    const idempotencyKey = crypto.randomUUID();
    send({
      type: "SUBMIT",
      payment,
      flow,
      idempotencyKey,
      timeoutMs,
    });
  };

  const stateName = String(state.value);
  const canEdit = state.matches("editing") || state.matches("failed") || state.matches("completed");
  const isPending = state.matches("submitting") || state.matches("cancelling_key");

  return (
    <main className="dashboard-grid">
      <section className="panel payment-panel">
        <div className="section-heading">
          <div>
            <p className="eyebrow">PAYMENT CONTROL</p>
            <h2>Send a payment</h2>
          </div>
          <span className={`status-pill ${isPending ? "busy" : stateName}`}>
            {stateName.replaceAll("_", " ")}
          </span>
        </div>

        <div className="flow-switch" aria-label="Payment type">
          <button
            type="button"
            className={flow === "p2p" ? "selected" : ""}
            disabled={!canEdit}
            onClick={() =>
              updatePayment(
                {
                  receiver_id:
                    payment.sender_id === "acc_asha" ? "acc_ravi" : "acc_asha",
                },
                "p2p",
              )
            }
          >
            P2P transfer
          </button>
          <button
            type="button"
            className={flow === "merchant" ? "selected" : ""}
            disabled={!canEdit}
            onClick={() => {
              updatePayment({ receiver_id: "acc_merchant_diwali" }, "merchant");
            }}
          >
            Flash sale
          </button>
        </div>

        <form
          onSubmit={(event) => {
            event.preventDefault();
            submit(5_000);
          }}
        >
          <label>
            Pay from
            <select
              value={payment.sender_id}
              disabled={!canEdit}
              onChange={(event) => {
                const senderId = event.target.value;
                updatePayment({
                  sender_id: senderId,
                  receiver_id:
                    flow === "p2p" && payment.receiver_id === senderId
                      ? senderId === "acc_asha"
                        ? "acc_ravi"
                        : "acc_asha"
                      : payment.receiver_id,
                });
              }}
            >
              <option value="acc_asha">Asha · acc_asha</option>
              <option value="acc_ravi">Ravi · acc_ravi</option>
            </select>
          </label>
          {flow === "p2p" ? (
            <label>
              Pay to
              <select
                value={payment.receiver_id}
                disabled={!canEdit}
                onChange={(event) => updatePayment({ receiver_id: event.target.value })}
              >
                <option value="acc_ravi">Ravi · acc_ravi</option>
                <option value="acc_asha">Asha · acc_asha</option>
              </select>
            </label>
          ) : (
            <div className="merchant-target">
              <span>Flash-sale merchant</span>
              <strong>Diwali Market</strong>
              <code>acc_merchant_diwali · 64 isolated credit slots</code>
            </div>
          )}
          <label>
            Amount <span className="field-note">USD · decimal precision 4</span>
            <div className="amount-input">
              <span>$</span>
              <input
                value={payment.amount}
                disabled={!canEdit}
                inputMode="decimal"
                autoComplete="off"
                onChange={(event) => updatePayment({ amount: event.target.value })}
                aria-label="Payment amount in USD"
              />
            </div>
          </label>
          <div className="button-row">
            {state.matches("editing") && (
              <>
                <button className="primary-button" type="submit">Submit securely</button>
                <button
                  className="quiet-button"
                  type="button"
                  onClick={() => submit(25)}
                >
                  Simulate timeout
                </button>
              </>
            )}
            {(state.matches("uncertain_timeout") || state.matches("processing_202")) && (
              <>
                <button className="primary-button" type="button" onClick={() => send({ type: "RETRY" })}>
                  Retry with same key
                </button>
                <button className="danger-button" type="button" onClick={() => send({ type: "CANCEL" })}>
                  Cancel key on server
                </button>
              </>
            )}
          </div>
        </form>

        {state.context.error && <p className="error-message" role="alert">{state.context.error}</p>}
        {state.context.result && (
          <div className="receipt">
            <span>{state.context.result.replayed ? "Idempotent replay" : "Payment confirmed"}</span>
            <strong>{state.context.result.payment_id}</strong>
            <span>{state.context.result.amount} USD · {state.context.result.status}</span>
          </div>
        )}
        {state.context.idempotencyKey && (
          <p className="key-line">
            Idempotency · <strong>{state.context.keyStatus}</strong>
            <code>{state.context.idempotencyKey}</code>
          </p>
        )}
      </section>

      <aside className="panel live-panel">
        <div className="section-heading">
          <div>
            <p className="eyebrow">REAL-TIME STREAM</p>
            <h2>Account activity</h2>
          </div>
          <span className={`live-indicator ${streamState === "Live" ? "online" : ""}`}>
            {streamState}
          </span>
        </div>
        <p className="account-chip">{payment.sender_id}</p>
        <div className="balance-card">
        <span>Available balance · {payment.sender_id}</span>
        <strong>{availableBalance ?? "—"} <small>USD</small></strong>
        {balanceError && <small className="balance-error">{balanceError}</small>}
        </div>
        {streamEvent ? (
          <div className="event-card">
            <span className="event-dot" />
            <div>
              <strong>Payment event received</strong>
              <p>{streamEvent.amount} USD · {streamEvent.sender_id} → {streamEvent.receiver_id}</p>
              <code>{streamEvent.payment_id}</code>
            </div>
          </div>
        ) : (
          <div className="empty-stream">
            <span className="pulse-ring" />
            <p>Waiting for signed payment events</p>
            <small>Outbox relay · HMAC audit chain · WebSocket</small>
          </div>
        )}
        <div className="safety-note">
          <span>●</span>
          <p>Amounts are sent as decimal strings. The server owns the balance and idempotency record.</p>
        </div>
      </aside>
    </main>
  );
}
