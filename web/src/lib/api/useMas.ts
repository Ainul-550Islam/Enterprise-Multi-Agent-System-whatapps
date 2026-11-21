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
    // query is represented by queryKey so identity changes retrigger.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [path, queryKey, nonce]);

  const reload = useCallback(() => setNonce((value) => value + 1), []);
  return { data, error, loading, reload };
}
