"use client";

import { useCallback, useEffect, useState } from "react";

import { masFetch } from "./client";
import { StableApiError } from "./types";

export interface MasState<T> {
  data: T | null;
  error: StableApiError | null;
  loading: boolean;
  reload: () => void;
}

/** Loads one BFF-proxied resource with real loading / error / empty states. */
export function useMas<T>(path: string | null, query?: Record<string, string | number | undefined>): MasState<T> {
  const [data, setData] = useState<T | null>(null);
  const [error, setError] = useState<StableApiError | null>(null);
  const [loading, setLoading] = useState(Boolean(path));
  const [nonce, setNonce] = useState(0);
  const queryKey = JSON.stringify(query ?? {});

  useEffect(() => {
    if (!path) {
      setLoading(false);
      return;
    }
    let cancelled = false;
    setLoading(true);
    void masFetch<T>(path, { query })
      .then((result) => {
        if (cancelled) {
          return;
        }
        setData(result);
        setError(null);
      })
      .catch((caught: unknown) => {
        if (cancelled) {
          return;
        }
        setError(
          caught instanceof StableApiError
            ? caught
            : new StableApiError({ code: "CLIENT", message: String(caught) }),
        );
      })
      .finally(() => {
        if (!cancelled) {
          setLoading(false);
        }
      });
    return () => {
      cancelled = true;
    };
    const onRefresh = () => setNonce((value) => value + 1);
    window.addEventListener("mas:refresh", onRefresh);
    // query is represented by queryKey so identity changes retrigger.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [path, queryKey, nonce]);

  const reload = useCallback(() => setNonce((value) => value + 1), []);
  return { data, error, loading, reload };
}

/**
 * Mutations through the same BFF client as reads, so CSRF, scope and envelope
 * handling stay in one place. Pass `refresh: true` to refetch `path`.
 */
export function useMasMutation<TResponse = unknown>(path: string | null) {
  const [error, setError] = useState<StableApiError | null>(null);
  const [pending, setPending] = useState(false);
  const [nonce, setNonce] = useState(0);

  const mutate = useCallback(
    async (
      method: "POST" | "PUT" | "PATCH" | "DELETE",
      target: string,
      body?: unknown,
      options?: { refresh?: boolean },
    ): Promise<TResponse | null> => {
      setPending(true);
      setError(null);
      try {
        const result = await masFetch<TResponse>(target, { method, body });
        if (options?.refresh) {
          window.dispatchEvent(new CustomEvent("mas:refresh", { detail: { path: target } }));
        }
        return result;
      } catch (caught: unknown) {
        setError(
          caught instanceof StableApiError
            ? caught
            : new StableApiError({ code: "CLIENT", message: String(caught) }),
        );
        return null;
      } finally {
        setPending(false);
      }
    },
    [],
  );

  return { mutate, error, pending, reload: () => setNonce((value) => value + 1), nonce, basePath: path };
}
