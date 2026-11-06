import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { enableAutoUnmount, flushPromises, mount } from '@vue/test-utils'

import EditingTemplatesView from '../EditingTemplatesView.vue'

enableAutoUnmount(afterEach)

const { listTemplates, getTemplate, getArtifact, approveTemplate, rejectTemplate, unlistTemplate, stepUpRun, showError } = vi.hoisted(() => ({
  listTemplates: vi.fn(),
  getTemplate: vi.fn(),
  getArtifact: vi.fn(),
  approveTemplate: vi.fn(),
  rejectTemplate: vi.fn(),
  unlistTemplate: vi.fn(),
  stepUpRun: vi.fn((action: () => Promise<unknown>) => action()),
  showError: vi.fn(),
}))

vi.mock('@/api/admin', () => ({
  adminAPI: {
    editingTemplates: {
      list: listTemplates,
      get: getTemplate,
      artifact: getArtifact,
      approve: approveTemplate,
      reject: rejectTemplate,
      unlist: unlistTemplate,
    },
  },
}))

vi.mock('@/stores', () => ({
  useAppStore: () => ({ showError, showSuccess: vi.fn() }),
}))

vi.mock('@/composables/useStepUp', () => ({
  useStepUp: () => ({ run: stepUpRun }),
  isStepUpBlocked: () => false,
  isStepUpCancelled: () => false,
  stepUpBlockReason: () => '',
}))

vi.mock('vue-i18n', async (importOriginal) => ({
  ...(await importOriginal<typeof import('vue-i18n')>()),
  useI18n: () => ({ t: (key: string) => key }),
}))

const template = {
  id: '3ac95d36-63dd-4e29-a7ea-8b51b0d99c0f',
  owner_id: 41,
  title: 'Launch story',
  description: 'A concise launch video',
  editing_brief: 'Open with the outcome.',
  aspect_ratio: '16:9',
  duration_ms: 45000,
  status: 'pending_review' as const,
  moderation_action: 'allow',
  artifacts: {
    project: { id: 1, template_id: 'id', kind: 'project' as const, original_filename: 'launch.ccproj', content_type: 'application/octet-stream', byte_size: 1024, sha256: 'a'.repeat(64), created_at: '', updated_at: '' },
    cover: { id: 2, template_id: 'id', kind: 'cover' as const, original_filename: 'cover.png', content_type: 'image/png', byte_size: 100, sha256: 'b'.repeat(64), created_at: '', updated_at: '' },
    preview: { id: 3, template_id: 'id', kind: 'preview' as const, original_filename: 'preview.mp4', content_type: 'video/mp4', byte_size: 2048, sha256: 'c'.repeat(64), created_at: '', updated_at: '' },
  },
  reviews: [],
  submitted_at: '2026-11-01T01:00:00Z',
  created_at: '2026-11-01T00:00:00Z',
  updated_at: '2026-11-01T01:00:00Z',
}

function mountView() {
  return mount(EditingTemplatesView, {
    global: {
      stubs: {
        AppLayout: { template: '<div><slot /></div>' },
        BaseDialog: { template: '<div><slot /><slot name="footer" /></div>' },
        TotpStepUpDialog: true,
        Icon: true,
      },
    },
  })
}

describe('EditingTemplatesView', () => {
  beforeEach(() => {
    vi.clearAllMocks()
    listTemplates.mockResolvedValue({ items: [template], total: 1, page: 1, page_size: 12, pages: 1 })
    getTemplate.mockResolvedValue(template)
    getArtifact.mockResolvedValue(new Blob(['asset']))
    approveTemplate.mockResolvedValue({ ...template, status: 'approved' })
    rejectTemplate.mockResolvedValue({ ...template, status: 'rejected' })
    unlistTemplate.mockResolvedValue({ ...template, status: 'unlisted' })
    stepUpRun.mockImplementation((action: () => Promise<unknown>) => action())
    vi.stubGlobal('URL', {
      ...URL,
      createObjectURL: vi.fn(() => 'blob:review-asset'),
      revokeObjectURL: vi.fn(),
    })
  })

  it('loads the pending queue and fetches preview assets when review opens', async () => {
    const wrapper = mountView()
    await flushPromises()

    expect(listTemplates).toHaveBeenCalledWith(expect.objectContaining({ status: 'pending_review' }))
    const reviewButton = wrapper.findAll('button').find(button => button.text() === 'admin.editingTemplates.view')
    expect(reviewButton).toBeDefined()
    await reviewButton!.trigger('click')
    await flushPromises()

    expect(getTemplate).toHaveBeenCalledWith(template.id)
    expect(getArtifact).toHaveBeenCalledWith(template.id, 'preview')
    expect(getArtifact).toHaveBeenCalledWith(template.id, 'cover')
    expect(wrapper.find('video').attributes('src')).toBe('blob:review-asset')
  })

  it('requires a rejection reason and executes review mutations through step-up', async () => {
    const wrapper = mountView()
    await flushPromises()
    await wrapper.findAll('button').find(button => button.text() === 'admin.editingTemplates.view')!.trigger('click')
    await flushPromises()

    const rejectButton = wrapper.findAll('button').find(button => button.text() === 'admin.editingTemplates.reject')!
    await rejectButton.trigger('click')
    expect(showError).toHaveBeenCalledWith('admin.editingTemplates.reasonRequired')
    expect(rejectTemplate).not.toHaveBeenCalled()

    await wrapper.get('textarea').setValue('Replace the placeholder frame')
    await rejectButton.trigger('click')
    await flushPromises()

    expect(stepUpRun).toHaveBeenCalledTimes(1)
    expect(rejectTemplate).toHaveBeenCalledWith(template.id, 'Replace the placeholder frame')
  })

  it('shows authenticated covers in the queue and releases them when the view closes', async () => {
    const wrapper = mountView()
    await flushPromises()

    expect(getArtifact).toHaveBeenCalledWith(template.id, 'cover')
    expect(getArtifact).not.toHaveBeenCalledWith(template.id, 'preview')
    expect(wrapper.get('article img').attributes('src')).toBe('blob:review-asset')
    wrapper.unmount()
    expect(URL.revokeObjectURL).toHaveBeenCalledWith('blob:review-asset')
  })

  it('keeps the selected filter when an older queue request completes later', async () => {
    let finishPending!: (value: unknown) => void
    listTemplates.mockImplementationOnce(() => new Promise(resolve => { finishPending = resolve }))
    listTemplates.mockResolvedValueOnce({ items: [{ ...template, title: 'Approved result', status: 'approved' }], total: 1, page: 1, page_size: 12, pages: 1 })
    const wrapper = mountView()
    await wrapper.findAll('button').find(button => button.text() === 'admin.editingTemplates.status.approved')!.trigger('click')
    await flushPromises()
    finishPending({ items: [template], total: 1, page: 1, page_size: 12, pages: 1 })
    await flushPromises()

    expect(wrapper.get('article h2').text()).toBe('Approved result')
    expect(wrapper.findAll('button').find(button => button.text() === 'admin.editingTemplates.status.approved')!.attributes('aria-pressed')).toBe('true')
  })
})
