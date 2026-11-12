import { PaymentForm } from "./components/PaymentForm";

export default function App() {
  return (
    <div className="app-shell">
      <header className="topbar">
        <a className="brand" href="/" aria-label="PayScale home">
          <span className="brand-mark">P</span>
          <span>payscale<span className="brand-muted"> / httpe</span></span>
        </a>
        <div className="topbar-meta">
          <span className="environment-tag">ENGINE 3.0</span>
          <span className="cluster-health"><i /> Systems operational</span>
        </div>
      </header>
      <section className="hero">
        <p className="eyebrow">HIGH-THROUGHPUT TRANSACTION PROCESSING</p>
        <h1>Payments, without the guesswork.</h1>
        <p>Atomic transfers, replay-safe retries, and live settlement signals.</p>
      </section>
      <PaymentForm />
      <footer>
        <span>PAYScale HTTPE 3.0</span>
        <span>Decimal-safe · Idempotent · Auditable</span>
      </footer>
    </div>
  );
}
