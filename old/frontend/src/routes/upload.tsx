import { createFileRoute } from "@tanstack/react-router";

import { UploadDialog } from "@/components/upload/UploadDialog";

export const Route = createFileRoute("/upload")({
  component: UploadDialog,
});
