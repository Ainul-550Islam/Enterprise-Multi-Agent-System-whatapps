"use client";

import type { ReactNode } from "react";

export interface DialogProps {
  title: string;
  open: boolean;
  onClose: () => void;
  children: ReactNode;
  footer?: ReactNode;
}

/** Modal dialog with focusable content and explicit close plumbing. */
export function Dialog({ title, open, onClose, children, footer }: DialogProps) {
  if (!open) {
    return null;
  }
  return (
    <div className="mas-dialog-backdrop" role="presentation" onClick={onClose}>
      <div
        className="mas-dialog"
        role="dialog"
        aria-modal="true"
        aria-label={title}
        onClick={(event) => event.stopPropagation()}
      >
        <header className="mas-dialog-header">
          <h2>{title}</h2>
          <button type="button" className="mas-dialog-close" aria-label="Close dialog" onClick={onClose}>
            ×
          </button>
        </header>
        <div className="mas-dialog-body">{children}</div>
        {footer ? <footer className="mas-dialog-footer">{footer}</footer> : null}
      </div>
    </div>
  );
}
