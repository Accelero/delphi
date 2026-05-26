// Public interface of the chat module. Internals (`MessageBody`, the smoothing
// hooks, the segment parser) are not re-exported — callers consume `<Chat>` and
// pass props.
export { Chat } from "./Chat";
export type { ChatProps } from "./Chat";
