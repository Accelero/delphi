import React from "react";
import { createRoot } from "react-dom/client";
import { App } from "./components/chat/App";
import "katex/dist/katex.min.css";
import "streamdown/styles.css";
import "./styles/globals.css";

createRoot(document.getElementById("root")!).render(
  <React.StrictMode>
    <App />
  </React.StrictMode>
);
