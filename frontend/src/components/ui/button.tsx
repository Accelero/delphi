import * as React from "react";
import { cn } from "../../lib/utils";

type ButtonProps = React.ButtonHTMLAttributes<HTMLButtonElement> & {
  variant?: "default" | "ghost" | "outline" | "destructive";
  size?: "default" | "icon" | "sm";
};

export function Button({
  className,
  variant = "default",
  size = "default",
  ...props
}: ButtonProps) {
  return (
    <button
      className={cn(
        "inline-flex items-center justify-center gap-2 rounded-md text-sm font-medium transition-colors focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-sky-500 disabled:pointer-events-none disabled:opacity-50",
        variant === "default" && "bg-stone-950 text-white hover:bg-stone-800",
        variant === "ghost" && "hover:bg-stone-100",
        variant === "outline" && "border border-stone-300 bg-white hover:bg-stone-50",
        variant === "destructive" && "bg-red-600 text-white hover:bg-red-700",
        size === "default" && "h-10 px-4 py-2",
        size === "sm" && "h-8 px-3",
        size === "icon" && "h-9 w-9",
        className
      )}
      {...props}
    />
  );
}
