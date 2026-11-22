"use client";

import { useRouter } from "next/navigation";
import { useEffect, useMemo, useRef, useState } from "react";

import { ErrorState } from "@/components/ui/error-state";
import { LoadingState } from "@/components/ui/loading";
import { EmptyState } from "@/components/ui/empty";
import { masFetch } from "@/lib/api/client";

interface SearchResult {
  kind: string;
  id: string;
  title: string;
  subtitle?: string | null;
}

interface SearchResponse {
  items: SearchResult[];
  truncated: boolean;
}

const RESULT_LIMIT = 8;
const MIN_QUERY_LENGTH = 3;
const DEBOUNCE_MS = 300;

function destination(result: SearchResult): string {
  const id = encodeURIComponent(result.id);
  switch (result.kind) {
    case "agents":
      return `/agents/${id}`;
    case "workflows":
      return `/workflows/${id}`;
    case "executions":
      return `/executions/${id}`;
    default:
      return "/connectors";
  }
}

export function SearchBox() {
  const router = useRouter();
  const [query, setQuery] = useState("");
  const [items, setItems] = useState<SearchResult[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<Error | null>(null);
  const [open, setOpen] = useState(false);
  const [activeIndex, setActiveIndex] = useState(0);
  const inputRef = useRef<HTMLInputElement>(null);
  const listId = "mas-search-results";

  useEffect(() => {
    const normalized = query.trim();
    if (normalized.length < MIN_QUERY_LENGTH) {
      setItems([]);
      setError(null);
      setLoading(false);
      return;
    }
    let cancelled = false;
    const controller = new AbortController();
    setLoading(true);
    setError(null);
    const timer = window.setTimeout(() => {
      void masFetch<SearchResponse>("/v1/search", {
        query: { q: normalized, kinds: "agents,workflows", limit: RESULT_LIMIT },
        signal: controller.signal,
      })
        .then((response) => {
          if (!cancelled) {
            setItems(response.items ?? []);
            setActiveIndex(0);
          }
        })
        .catch((caught: unknown) => {
          if (!cancelled && !(caught instanceof DOMException && caught.name === "AbortError")) {
            setError(caught instanceof Error ? caught : new Error(String(caught)));
            setItems([]);
          }
        })
        .finally(() => {
          if (!cancelled) setLoading(false);
        });
    }, DEBOUNCE_MS);
    return () => {
      cancelled = true;
      window.clearTimeout(timer);
      controller.abort();
    };
  }, [query]);

  const grouped = useMemo(() => {
    const groups = new Map<string, SearchResult[]>();
    for (const item of items) {
      const group = groups.get(item.kind) ?? [];
      group.push(item);
      groups.set(item.kind, group);
    }
    return [...groups.entries()];
  }, [items]);

  function navigate(result: SearchResult | undefined) {
    if (!result) return;
    router.push(destination(result));
    setOpen(false);
  }

  function onKeyDown(event: React.KeyboardEvent<HTMLInputElement>) {
    if (event.key === "Escape") {
      setOpen(false);
      return;
    }
    if (event.key === "ArrowDown" && items.length) {
      event.preventDefault();
      setActiveIndex((index) => (index + 1) % items.length);
      return;
    }
    if (event.key === "ArrowUp" && items.length) {
      event.preventDefault();
      setActiveIndex((index) => (index + items.length - 1) % items.length);
      return;
    }
    if (event.key === "Enter") {
      event.preventDefault();
      navigate(items[activeIndex] ?? items[0]);
    }
  }

  let flatIndex = 0;
  return (
    <div className="relative w-full max-w-sm" onBlur={(event) => {
      if (!event.currentTarget.contains(event.relatedTarget as Node | null)) setOpen(false);
    }}>
      <label className="sr-only" htmlFor="mas-global-search">Search agents and workflows</label>
      <input
        ref={inputRef}
        id="mas-global-search"
        role="combobox"
        aria-autocomplete="list"
        aria-controls={listId}
        aria-expanded={open}
        aria-activedescendant={open && items[activeIndex] ? `mas-search-option-${activeIndex}` : undefined}
        autoComplete="off"
        placeholder="Search agents and workflows…"
        className="w-full rounded border border-line bg-panel px-3 py-2 text-sm text-ink"
        value={query}
        onFocus={() => setOpen(true)}
        onChange={(event) => {
          setQuery(event.target.value);
          setOpen(true);
        }}
        onKeyDown={onKeyDown}
      />
      {open ? (
        <div id={listId} role="listbox" aria-label="Search results" className="absolute z-50 mt-1 max-h-96 w-full overflow-auto rounded border border-line bg-panel p-2 shadow-lg">
          {query.trim().length < MIN_QUERY_LENGTH ? (
            <p className="mas-muted px-2 py-1 text-sm">Type at least 3 characters.</p>
          ) : null}
          {loading ? <LoadingState label="Searching…" /> : null}
          {error ? <ErrorState error={error} /> : null}
          {!loading && !error && query.trim().length >= MIN_QUERY_LENGTH && items.length === 0 ? (
            <EmptyState title="No results" detail="Try another search." />
          ) : null}
          {!loading && items.length > 0 ? grouped.map(([kind, group]) => (
            <section key={kind} role="group" aria-label={kind} className="mb-2 last:mb-0">
              <h2 className="px-2 py-1 text-xs font-semibold uppercase text-muted">{kind}</h2>
              {group.map((item) => {
                const index = flatIndex++;
                return (
                  <button
                    key={`${item.kind}:${item.id}`}
                    id={`mas-search-option-${index}`}
                    type="button"
                    role="option"
                    aria-selected={activeIndex === index}
                    className="block w-full rounded px-2 py-2 text-left text-sm hover:bg-canvas"
                    onMouseEnter={() => setActiveIndex(index)}
                    onClick={() => navigate(item)}
                  >
                    <span className="block font-medium">{item.title}</span>
                    {item.subtitle ? <span className="block text-xs text-muted">{item.subtitle}</span> : null}
                  </button>
                );
              })}
            </section>
          )) : null}
        </div>
      ) : null}
    </div>
  );
}
