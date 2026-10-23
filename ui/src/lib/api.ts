import type { CreateAnswer } from "@/api/CreateAnswer";
import type { ErrorAnswer } from "@/api/ErrorAnswer";
import type { FolderRequest } from "@/api/FolderRequest";
import type { InviteAnswer } from "@/api/InviteAnswer";
import type { InviteRequest } from "@/api/InviteRequest";
import type { JoinAnswer } from "@/api/JoinAnswer";
import type { JoinRequest } from "@/api/JoinRequest";
import type { MemberRequest } from "@/api/MemberRequest";
import type { QueuedAnswer } from "@/api/QueuedAnswer";

/** POSTs an action. A non-2xx answer throws with its `error` text. */
async function post<T>(route: string, body: unknown): Promise<T> {
  const response = await fetch(`/api${route}`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  const answer: unknown = await response.json().catch(() => null);
  if (!response.ok) {
    throw new Error((answer as ErrorAnswer | null)?.error ?? `${response.status} ${response.statusText}`);
  }
  return answer as T;
}

export const createGroup = () => post<CreateAnswer>("/group", {});
export const join = (request: JoinRequest) => post<JoinAnswer>("/join", request);
export const createInvite = (request: InviteRequest) => post<InviteAnswer>("/invites", request);
export const addMember = (request: MemberRequest) => post<QueuedAnswer>("/members", request);
export const addFolder = (request: FolderRequest) => post<QueuedAnswer>("/folders", request);
