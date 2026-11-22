"use client";

import type { TextareaHTMLAttributes } from "react";

export interface TextareaProps extends TextareaHTMLAttributes<HTMLTextAreaElement> {
  label: string;
  error?: string | undefined;
}

/** Labelled textarea with the same inline validation contract as Input. */
export function Textarea({ label, error, id, ...rest }: TextareaProps) {
  const textareaId = id ?? `textarea-${label.toLowerCase().replace(/\W+/g, "-")}`;
  return (
    <div className={`mas-field${error ? " mas-field-invalid" : ""}`}>
      <label htmlFor={textareaId}>{label}</label>
      <textarea
        id={textareaId}
        aria-invalid={error ? true : undefined}
        aria-describedby={error ? `${textareaId}-error` : undefined}
        {...rest}
      />
      {error ? (
        <p id={`${textareaId}-error`} role="alert" className="mas-field-error">
          {error}
        </p>
      ) : null}
    </div>
  );
}
