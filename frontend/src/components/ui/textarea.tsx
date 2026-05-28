import * as React from "react";
import { cn } from "../../lib/utils";

export function Textarea({
  className,
  ...props
}: React.TextareaHTMLAttributes<HTMLTextAreaElement>) {
  return (
    <textarea
      className={cn(
        "min-h-24 w-full resize-none rounded-3xl border-0 bg-[var(--color-object)] px-4 py-3 text-sm leading-6 text-[var(--color-object-text)] outline-none placeholder:text-[var(--color-text-subtle)] focus:ring-2 focus:ring-[var(--color-focus)]",
        className
      )}
      {...props}
    />
  );
}
