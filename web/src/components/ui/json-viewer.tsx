export function JsonViewer({ value }: { value: unknown }) {
  return <pre className="mas-json">{JSON.stringify(value, null, 2)}</pre>;
}
