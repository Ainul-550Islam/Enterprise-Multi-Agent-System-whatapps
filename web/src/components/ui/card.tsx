"use client";

import type { ReactNode } from "react";

export interface CardProps {
  title: string;
  children: ReactNode;
  footer?: ReactNode;
  /** When set, the card renders its body in the loading state. */
  loading?: boolean;
}

/** Section card with a title bar and optional footer. */
export function Card({ title, children, footer, loading = false }: CardProps) {
  return (
    <section className="mas-card" aria-label={title}>
      <header className="mas-card-header">
        <h2>{title}</h2>
      </header>
      <div className="mas-card-body">{loading ? <p className="mas-muted">Loading…</p> : children}</div>
      {footer ? <footer className="mas-card-footer">{footer}</footer> : null}
    </section>
  );
}
