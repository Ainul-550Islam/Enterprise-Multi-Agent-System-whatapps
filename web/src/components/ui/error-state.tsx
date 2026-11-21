import { StableApiError } from "@/lib/api/types";

export function ErrorState({ error }: { error: StableApiError | Error | string }) {
  const code = error instanceof StableApiError ? error.code : undefined;
  const message = typeof error === "string" ? error : error.message;
  return (
    <div className="mas-error" role="alert">
      {code ? <strong className="mr-2">{code}</strong> : null}
      {message}
    </div>
  );
}
