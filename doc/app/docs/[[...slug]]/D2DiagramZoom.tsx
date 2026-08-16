'use client';

import type { CSSProperties, ReactNode } from 'react';
import { useEffect, useRef, useState } from 'react';

const closeButtonIdleMs = 1200;
const desktopModalMarginPx = 40;
const mobileModalMarginPx = 8;
const modalBorderPx = 2;

type DiagramZoomContent = {
  html: string;
  intrinsicHeight: number;
  intrinsicWidth: number;
};

type Size = {
  height: number;
  width: number;
};

const fallbackDiagramSize: Size = {
  height: 720,
  width: 1080,
};

export function D2DiagramZoom({ children }: { children: ReactNode }) {
  const rootRef = useRef<HTMLDivElement>(null);
  const dialogRef = useRef<HTMLDivElement>(null);
  const closeButtonRef = useRef<HTMLButtonElement>(null);
  const closeButtonTimerRef = useRef<ReturnType<typeof setTimeout> | undefined>(
    undefined,
  );
  const [diagram, setDiagram] = useState<DiagramZoomContent | undefined>();
  const [isCloseButtonVisible, setIsCloseButtonVisible] = useState(false);
  const [viewportSize, setViewportSize] = useState<Size>({
    height: 0,
    width: 0,
  });

  useEffect(() => {
    const root = rootRef.current;

    if (!root) {
      return;
    }

    const diagrams = root.querySelectorAll<HTMLElement>(
      '.d2-diagram:not(.d2-diagram-error)',
    );

    for (const diagram of diagrams) {
      diagram.tabIndex = 0;
      diagram.setAttribute('role', 'button');
      diagram.setAttribute('aria-label', 'Open enlarged diagram');
    }

    function openDiagram(target: EventTarget | null): void {
      if (!(target instanceof Element)) {
        return;
      }

      const diagram = target.closest<HTMLElement>(
        '.d2-diagram:not(.d2-diagram-error)',
      );

      if (!diagram || !root?.contains(diagram)) {
        return;
      }

      const intrinsicSize = getDiagramIntrinsicSize(diagram);

      setViewportSize(getViewportSize());
      setDiagram({
        html: diagram.innerHTML,
        intrinsicHeight: intrinsicSize.height,
        intrinsicWidth: intrinsicSize.width,
      });
    }

    function onClick(event: MouseEvent): void {
      openDiagram(event.target);
    }

    function onKeyDown(event: KeyboardEvent): void {
      if (event.key !== 'Enter' && event.key !== ' ') {
        return;
      }

      const target = event.target;

      if (
        target instanceof Element &&
        target.closest('.d2-diagram:not(.d2-diagram-error)')
      ) {
        event.preventDefault();
        openDiagram(target);
      }
    }

    root.addEventListener('click', onClick);
    root.addEventListener('keydown', onKeyDown);

    return () => {
      root.removeEventListener('click', onClick);
      root.removeEventListener('keydown', onKeyDown);
    };
  }, [children]);

  useEffect(() => {
    if (!diagram) {
      return;
    }

    const previousOverflow = document.body.style.overflow;

    document.body.style.overflow = 'hidden';
    setIsCloseButtonVisible(false);
    dialogRef.current?.focus();

    function onKeyDown(event: KeyboardEvent): void {
      if (event.key === 'Escape') {
        setDiagram(undefined);
      }
    }

    function onResize(): void {
      setViewportSize(getViewportSize());
    }

    window.addEventListener('keydown', onKeyDown);
    window.addEventListener('resize', onResize);

    return () => {
      document.body.style.overflow = previousOverflow;
      window.removeEventListener('keydown', onKeyDown);
      window.removeEventListener('resize', onResize);
    };
  }, [diagram]);

  useEffect(() => {
    return () => {
      if (closeButtonTimerRef.current) {
        clearTimeout(closeButtonTimerRef.current);
      }
    };
  }, []);

  function showCloseButtonTemporarily(): void {
    if (closeButtonTimerRef.current) {
      clearTimeout(closeButtonTimerRef.current);
    }

    setIsCloseButtonVisible(true);
    closeButtonTimerRef.current = setTimeout(() => {
      setIsCloseButtonVisible(false);
    }, closeButtonIdleMs);
  }

  function hideCloseButton(): void {
    if (closeButtonTimerRef.current) {
      clearTimeout(closeButtonTimerRef.current);
    }

    setIsCloseButtonVisible(false);
  }

  function closeDiagram(): void {
    hideCloseButton();
    setDiagram(undefined);
  }

  const panelSize = diagram
    ? getScrollablePanelSize(
        {
          height: diagram.intrinsicHeight,
          width: diagram.intrinsicWidth,
        },
        viewportSize,
      )
    : undefined;
  const panelStyle: CSSProperties | undefined = panelSize
    ? {
        height: `${panelSize.height}px`,
        '--d2-zoom-intrinsic-height': `${diagram?.intrinsicHeight}px`,
        '--d2-zoom-intrinsic-width': `${diagram?.intrinsicWidth}px`,
        width: `${panelSize.width}px`,
      } as CSSProperties
    : undefined;

  return (
    <>
      <div ref={rootRef}>{children}</div>
      {diagram ? (
        <div
          className="d2-diagram-modal"
          role="presentation"
          onClick={closeDiagram}
        >
          <div
            ref={dialogRef}
            aria-label="Enlarged diagram"
            aria-modal="true"
            className="d2-diagram-modal-panel"
            role="dialog"
            style={panelStyle}
            tabIndex={-1}
            onPointerLeave={hideCloseButton}
            onPointerMove={showCloseButtonTemporarily}
            onClick={(event) => event.stopPropagation()}
          >
            <button
              ref={closeButtonRef}
              aria-label="Close enlarged diagram"
              className={
                isCloseButtonVisible
                  ? 'd2-diagram-modal-close d2-diagram-modal-close-visible'
                  : 'd2-diagram-modal-close'
              }
              type="button"
              onClick={closeDiagram}
            >
              X
            </button>
            <figure
              className="d2-diagram d2-diagram-expanded"
              onClick={closeDiagram}
              dangerouslySetInnerHTML={{ __html: diagram.html }}
            />
          </div>
        </div>
      ) : null}
    </>
  );
}

function getViewportSize(): Size {
  return {
    height: window.innerHeight,
    width: window.innerWidth,
  };
}

function getScrollablePanelSize(diagramSize: Size, viewportSize: Size): Size {
  const viewportWidth = Math.max(viewportSize.width, 1);
  const viewportHeight = Math.max(viewportSize.height, 1);
  const modalMargin =
    viewportWidth <= 640 ? mobileModalMarginPx : desktopModalMarginPx;
  const maxWidth = Math.max(viewportWidth - modalMargin * 2 - modalBorderPx, 1);
  const maxHeight = Math.max(
    viewportHeight - modalMargin * 2 - modalBorderPx,
    1,
  );

  return {
    height: Math.round(Math.min(diagramSize.height, maxHeight)),
    width: Math.round(Math.min(diagramSize.width, maxWidth)),
  };
}

function getDiagramIntrinsicSize(diagram: HTMLElement): Size {
  const svg = findVisibleDiagramSvg(diagram);

  if (!svg) {
    return fallbackDiagramSize;
  }

  const width = parseSvgLength(svg.getAttribute('width'));
  const height = parseSvgLength(svg.getAttribute('height'));

  if (width && height) {
    return { height, width };
  }

  const viewBox = svg.getAttribute('viewBox');
  const viewBoxSize = parseViewBoxSize(viewBox);

  if (viewBoxSize) {
    return viewBoxSize;
  }

  const box = svg.getBoundingClientRect();

  if (box.width > 0 && box.height > 0) {
    return {
      height: box.height,
      width: box.width,
    };
  }

  return fallbackDiagramSize;
}

function findVisibleDiagramSvg(diagram: HTMLElement): SVGSVGElement | undefined {
  const svgs = diagram.querySelectorAll<SVGSVGElement>('svg');

  for (const svg of svgs) {
    if (svg.getClientRects().length > 0) {
      return svg;
    }
  }

  return svgs[0];
}

function parseSvgLength(value: string | null): number | undefined {
  if (!value) {
    return undefined;
  }

  const match = value.trim().match(/^(\d+(?:\.\d+)?)(?:px)?$/);

  if (!match) {
    return undefined;
  }

  return Number(match[1]);
}

function parseViewBoxSize(value: string | null): Size | undefined {
  if (!value) {
    return undefined;
  }

  const parts = value
    .trim()
    .split(/[\s,]+/)
    .map((part) => Number(part));

  if (parts.length !== 4 || parts.some((part) => !Number.isFinite(part))) {
    return undefined;
  }

  const [, , width, height] = parts;

  if (width <= 0 || height <= 0) {
    return undefined;
  }

  return { height, width };
}
