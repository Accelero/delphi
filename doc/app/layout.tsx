import './global.css';
import type { Metadata } from 'next';
import { RootProvider } from 'fumadocs-ui/provider/next';
import type { ReactNode } from 'react';

export const metadata: Metadata = {
  title: {
    default: 'Delphi Docs',
    template: '%s | Delphi Docs',
  },
  description:
    'Architecture, runtime workflows, and operating notes for the Delphi project.',
};

export default function RootLayout({ children }: { children: ReactNode }) {
  return (
    <html lang="en" suppressHydrationWarning>
      <body>
        <RootProvider
          search={{
            enabled: false,
          }}
          theme={{
            defaultTheme: 'dark',
          }}
        >
          {children}
        </RootProvider>
      </body>
    </html>
  );
}
