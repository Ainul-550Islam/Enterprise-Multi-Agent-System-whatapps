"use client";

import type { InputHTMLAttributes } from "react";

export interface InputProps extends InputHTMLAttributes<HTMLInputElement> {
  label: string;
  /** Inline validation message; when set the input renders invalid. */
  error?: string | undefined;
}

/** Labelled input with inline validation state. */
export function Input({ label, error, id, ...rest }: InputProps) {
  const inputId = id ?? `input-${label.toLowerCase().replace(/\W+/g, "-")}`;
  return (
    <div className={`mas-field${error ? " mas-field-invalid" : ""}`}>
      <label htmlFor={inputId}>{label}</label>
      <input
        id={inputId}
        aria-invalid={error ? true : undefined}
        aria-describedby={error ? `${inputId}-error` : undefined}
        {...rest}
      />
      {error ? (
        <p id={`${inputId}-error`} role="alert" className="mas-field-error">
          {error}
        </p>
      ) : null}
    </div>
  );
}
