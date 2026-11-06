import { apiClient } from '../client'

export type EditingTemplateStatus =
  | 'draft'
  | 'pending_review'
  | 'approved'
  | 'rejected'
  | 'unlisted'

export type EditingTemplateArtifactKind = 'project' | 'cover' | 'preview'

export interface EditingTemplateArtifact {
  id: number
  template_id: string
  kind: EditingTemplateArtifactKind
  original_filename: string
  content_type: string
  byte_size: number
  sha256: string
  created_at: string
  updated_at: string
}

export interface EditingTemplateReview {
  id: number
  template_id: string
  actor_id?: number
  action: 'submit' | 'approve' | 'reject' | 'unlist'
  from_status: EditingTemplateStatus
  to_status: EditingTemplateStatus
  reason?: string
  created_at: string
}

export interface EditingTemplate {
  id: string
  owner_id: number
  title: string
  description: string
  editing_brief: string
  aspect_ratio: string
  duration_ms: number
  status: EditingTemplateStatus
  rejection_reason?: string
  moderation_action?: string
  artifacts: Partial<Record<EditingTemplateArtifactKind, EditingTemplateArtifact>>
  reviews?: EditingTemplateReview[]
  submitted_at?: string
  reviewed_at?: string
  published_at?: string
  rejected_at?: string
  unlisted_at?: string
  created_at: string
  updated_at: string
}

export interface EditingTemplateListResponse {
  items: EditingTemplate[]
  total: number
  page: number
  page_size: number
  pages: number
}

export interface EditingTemplateListParams {
  status?: EditingTemplateStatus | ''
  search?: string
  page?: number
  page_size?: number
}

export async function list(params: EditingTemplateListParams): Promise<EditingTemplateListResponse> {
  const { data } = await apiClient.get<EditingTemplateListResponse>('/admin/editing-templates', { params })
  return data
}

export async function get(id: string): Promise<EditingTemplate> {
  const { data } = await apiClient.get<EditingTemplate>(`/admin/editing-templates/${id}`)
  return data
}

export async function approve(id: string): Promise<EditingTemplate> {
  const { data } = await apiClient.post<EditingTemplate>(`/admin/editing-templates/${id}/approve`)
  return data
}

export async function reject(id: string, reason: string): Promise<EditingTemplate> {
  const { data } = await apiClient.post<EditingTemplate>(`/admin/editing-templates/${id}/reject`, { reason })
  return data
}

export async function unlist(id: string, reason: string): Promise<EditingTemplate> {
  const { data } = await apiClient.post<EditingTemplate>(`/admin/editing-templates/${id}/unlist`, { reason })
  return data
}

export async function artifact(id: string, kind: EditingTemplateArtifactKind): Promise<Blob> {
  const { data } = await apiClient.get<Blob>(`/admin/editing-templates/${id}/artifacts/${kind}`, {
    responseType: 'blob',
    timeout: 120000
  })
  return data
}

export default { list, get, approve, reject, unlist, artifact }
