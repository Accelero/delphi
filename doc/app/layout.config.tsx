import type { BaseLayoutProps } from 'fumadocs-ui/layouts/shared';

export const baseOptions: BaseLayoutProps = {
  nav: {
    title: 'Delphi Docs',
    url: '/docs',
  },
  links: [
    {
      text: 'Overview',
      url: '/docs',
      active: 'nested-url',
    },
  ],
};
