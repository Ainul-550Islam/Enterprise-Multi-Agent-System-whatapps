"use client";

import type { ChangeEvent, InputHTMLAttributes } from "react";

export interface CheckboxProps extends Omit<InputHTMLAttributes<HTMLInputElement>, "onChange"> {
  label: string;
  onCheckedChange?: (checked: boolean) => void;
}

/** Labelled checkbox with an explicit change callback. */
export function Checkbox({ label, onCheckedChange, ...props }: CheckboxProps) {
  return (
    <label className="flex items-center gap-2 text-sm">
      <input
        type="checkbox"
        {...props}
        onChange={(event: ChangeEvent<HTMLInputElement>) => {
          onCheckedChange?.(event.target.checked);
        }}
      />
      {label}
    </label>
  );
}
