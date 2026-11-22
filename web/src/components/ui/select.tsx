"use client";

import type { SelectHTMLAttributes } from "react";

export interface SelectOption {
  value: string;
  label: string;
}

/** Labelled native select; the label is always rendered for a11y. */
export function Select({
  label,
  options,
  ...props
}: SelectHTMLAttributes<HTMLSelectElement> & { label: string; options: SelectOption[] }) {
  return (
    <label className="mas-muted text-xs">
      {label}
      <select className="ml-2 rounded border border-line bg-panel px-2 py-1 text-ink" {...props}>
        {options.map((option) => (
          <option key={option.value} value={option.value}>
            {option.label}
          </option>
        ))}
      </select>
    </label>
  );
}
