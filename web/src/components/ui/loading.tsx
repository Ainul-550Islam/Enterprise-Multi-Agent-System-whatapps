export function LoadingState({ label = "Loading…" }: { label?: string }) {
  return (
    <p className="mas-muted" role="status" aria-live="polite">
      {label}
    </p>
  );
}
