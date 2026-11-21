export function EmptyState({ title, detail }: { title: string; detail?: string }) {
  return (
    <div className="mas-muted py-6 text-center" role="status">
      <p className="m-0 font-medium text-ink">{title}</p>
      {detail ? <p className="m-0 mt-1">{detail}</p> : null}
    </div>
  );
}
