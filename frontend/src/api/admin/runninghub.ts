import { apiClient } from '../client'

export type RunningHubTier = 'consumer' | 'enterprise'

export interface RunningHubAccount {
  id: number
  name: string
  tier: RunningHubTier
  max_concurrency: number
  enabled: boolean
  in_flight: number
}

export interface RunningHubAccountCreateRequest {
  name: string
  tier: RunningHubTier
  api_key: string
  max_concurrency: number
}

export interface RunningHubAccountUpdateRequest {
  name?: string
  max_concurrency?: number
  enabled?: boolean
  api_key?: string
}

export interface RunningHubSettings {
  consumer_enabled: boolean
  enterprise_enabled: boolean
  yuan_to_coin: number
}

export interface RunningHubNode {
  nodeId: string
  nodeName?: string
  fieldName: string
  fieldValue: unknown
  fieldType?: string
  description?: string
  [key: string]: unknown
}

export interface RunningHubWorkflow {
  id: number
  webapp_id: string
  name: string
  description: string
  instance_type: string
  estimated_rh: number
  avg_running_seconds: number
  node_info_list: RunningHubNode[]
  enabled: boolean
  sort_order: number
}

export interface RunningHubWorkflowCreateRequest {
  webapp_id: string
  name?: string
  description?: string
  instance_type?: string
  estimated_rh?: number
}

export type RunningHubWorkflowUpdateRequest = Partial<
  Omit<RunningHubWorkflow, 'id' | 'webapp_id'>
>

export interface RunningHubMedia {
  id: string
  ref: string
  file_name: string
  content_type: string
  file_type: string
  size_bytes: number
}

const base = '/admin/runninghub'

// ── Settings ──────────────────────────────────────────────────────────────────

export async function getSettings(): Promise<RunningHubSettings> {
  const { data } = await apiClient.get<RunningHubSettings>(`${base}/settings`)
  return data
}

export async function updateSettings(req: RunningHubSettings): Promise<RunningHubSettings> {
  const { data } = await apiClient.put<RunningHubSettings>(`${base}/settings`, req)
  return data
}

// ── Accounts ──────────────────────────────────────────────────────────────────

export async function listAccounts(): Promise<RunningHubAccount[]> {
  const { data } = await apiClient.get<RunningHubAccount[] | null>(`${base}/accounts`)
  return data ?? []
}

export async function addAccount(req: RunningHubAccountCreateRequest): Promise<{ id: number }> {
  const { data } = await apiClient.post<{ id: number }>(`${base}/accounts`, req)
  return data
}

export async function updateAccount(id: number, req: RunningHubAccountUpdateRequest): Promise<void> {
  await apiClient.patch(`${base}/accounts/${id}`, req)
}

export async function deleteAccount(id: number): Promise<void> {
  await apiClient.delete(`${base}/accounts/${id}`)
}

// ── Workflows ─────────────────────────────────────────────────────────────────

export async function listWorkflows(): Promise<RunningHubWorkflow[]> {
  const { data } = await apiClient.get<RunningHubWorkflow[] | null>(`${base}/workflows`)
  return data ?? []
}

export async function createWorkflow(req: RunningHubWorkflowCreateRequest): Promise<RunningHubWorkflow> {
  const { data } = await apiClient.post<RunningHubWorkflow>(`${base}/workflows`, req)
  return data
}

export async function updateWorkflow(
  id: number,
  req: RunningHubWorkflowUpdateRequest,
): Promise<RunningHubWorkflow> {
  const { data } = await apiClient.patch<RunningHubWorkflow>(`${base}/workflows/${id}`, req)
  return data
}

export async function refreshWorkflow(id: number): Promise<RunningHubWorkflow> {
  const { data } = await apiClient.post<RunningHubWorkflow>(`${base}/workflows/${id}/refresh`)
  return data
}

export async function deleteWorkflow(id: number): Promise<void> {
  await apiClient.delete(`${base}/workflows/${id}`)
}

export async function uploadMedia(file: File): Promise<RunningHubMedia> {
  const form = new FormData()
  form.append('file', file)
  const { data } = await apiClient.post<RunningHubMedia>(`${base}/media`, form, {
    headers: { 'Content-Type': 'multipart/form-data' },
    timeout: 10 * 60 * 1000,
  })
  return data
}

// Fetches a staged media file with admin auth so it can be previewed via a blob URL.
export async function getMediaFile(id: string): Promise<Blob> {
  const { data } = await apiClient.get<Blob>(`${base}/media/${encodeURIComponent(id)}/file`, {
    responseType: 'blob',
    timeout: 10 * 60 * 1000,
  })
  return data
}

// ── Credits ───────────────────────────────────────────────────────────────────

export async function getUserCredits(userId: number): Promise<{ rh_credits: number }> {
  const { data } = await apiClient.get<{ rh_credits: number }>(`${base}/users/${userId}/rh-credits`)
  return data
}

export async function grantRHCredits(userId: number, amount: number): Promise<{ rh_credits: number }> {
  const { data } = await apiClient.post<{ rh_credits: number }>(`${base}/users/${userId}/rh-credits`, {
    amount,
  })
  return data
}
