import * as React from "react";
import { cn } from "../../lib/utils";

export function Textarea({
  className,
  ...props
}: React.TextareaHTMLAttributes<HTMLTextAreaElement>) {
  return (
    <textarea
      className={cn(
        "min-h-24 w-full resize-none rounded-md border border-stone-300 bg-white px-3 py-2 text-sm leading-6 outline-none focus:ring-2 focus:ring-sky-500",
        className
      )}
      {...props}
    />
  );
}
