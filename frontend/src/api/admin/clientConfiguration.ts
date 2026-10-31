import { apiClient } from '../client'

export interface ClientConfiguration {
  schemaVersion: 1
  instanceId: string
  minClientVersion: string
  defaults: Record<string, string>
  routes: Array<{ groupIds: number[]; route: Record<string, unknown> }>
}

export async function getClientConfiguration(): Promise<ClientConfiguration> {
  const { data } = await apiClient.get<ClientConfiguration>('/admin/client-config')
  return data
}

export async function saveClientConfiguration(input: unknown): Promise<ClientConfiguration> {
  const { data } = await apiClient.put<ClientConfiguration>('/admin/client-config', input)
  return data
}
