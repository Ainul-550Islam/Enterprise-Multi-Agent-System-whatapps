"use client";

import type { ButtonHTMLAttributes, ReactNode } from "react";

export type ButtonTone = "primary" | "danger" | "ghost";

export interface ButtonProps extends ButtonHTMLAttributes<HTMLButtonElement> {
  tone?: ButtonTone;
  busy?: boolean;
  children: ReactNode;
}

const toneClass: Record<ButtonTone, string> = {
  primary: "mas-btn mas-btn-primary",
  danger: "mas-btn mas-btn-danger",
  ghost: "mas-btn mas-btn-ghost",
};

/** The dashboard's button: real click plumbing, busy state, tone styling. */
export function Button({ tone = "primary", busy = false, children, disabled, ...rest }: ButtonProps) {
  return (
    <button
      type={rest.type ?? "button"}
      className={`${toneClass[tone]}${busy ? " mas-btn-busy" : ""}`}
      disabled={disabled || busy}
      aria-busy={busy || undefined}
      {...rest}
    >
      {busy ? "Working…" : children}
    </button>
  );
}
