import { flushPromises, mount } from '@vue/test-utils'
import { beforeEach, describe, expect, it, vi } from 'vitest'
import AutoDesignClientSettings from '../AutoDesignClientSettings.vue'
import en from '@/i18n/locales/en/clientConfiguration'
import zh from '@/i18n/locales/zh/clientConfiguration'

const mocks = vi.hoisted(() => ({ getClientConfiguration: vi.fn(), saveClientConfiguration: vi.fn() }))
vi.mock('@/api/admin/clientConfiguration', () => mocks)
vi.mock('vue-i18n', () => ({ useI18n: () => ({ t: (key: string, values: Record<string, unknown> = {}) => {
  const message = en[key.replace('clientConfiguration.', '') as keyof typeof en]
  if (!message) throw new Error(`Missing message: ${key}`)
  return message.replace(/\{(\w+)\}/g, (_, name: string) => String(values[name]))
} }) }))

const config = { schemaVersion: 1, instanceId: 'sub-test', minClientVersion: '0.25.1', defaults: {}, routes: [] }
beforeEach(() => {
  vi.clearAllMocks()
  mocks.getClientConfiguration.mockResolvedValue(structuredClone(config))
  mocks.saveClientConfiguration.mockImplementation(async value => value)
})

describe('AutoDesign client publication', () => {
  it('has matching English and Chinese messages', () => { expect(Object.keys(en).sort()).toEqual(Object.keys(zh).sort()) })

  it('loads an empty directory and publishes without submitting the surrounding settings form', async () => {
    const wrapper = mount(AutoDesignClientSettings)
    await flushPromises()
    expect(wrapper.text()).toContain(en.empty)
    expect(wrapper.get('textarea').element.value).toContain('sub-test')
    expect(wrapper.get('[data-test="save-client-configuration"]').attributes('type')).toBe('button')
    await wrapper.get('[data-test="save-client-configuration"]').trigger('click')
    await flushPromises()
    expect(mocks.saveClientConfiguration).toHaveBeenCalledWith(config)
    expect(wrapper.get('[role="status"]').text()).toBe(en.saved)
  })

  it('keeps publication disabled after an initial load failure and can retry', async () => {
    mocks.getClientConfiguration.mockRejectedValueOnce(new Error('Load fixture failed'))
    const wrapper = mount(AutoDesignClientSettings)
    await flushPromises()
    expect(wrapper.get('[role="alert"]').text()).toBe(en.loadError)
    expect(wrapper.get('[data-test="save-client-configuration"]').attributes('disabled')).toBeDefined()
    await wrapper.get('[data-test="reload-client-configuration"]').trigger('click')
    await flushPromises()
    expect(wrapper.get('textarea').exists()).toBe(true)
    expect(wrapper.get('[data-test="save-client-configuration"]').attributes('disabled')).toBeUndefined()
  })

  it('rejects invalid JSON locally and preserves the editor after a server validation error', async () => {
    const wrapper = mount(AutoDesignClientSettings)
    await flushPromises()
    await wrapper.get('textarea').setValue('{ invalid')
    await wrapper.get('[data-test="save-client-configuration"]').trigger('click')
    expect(wrapper.get('[role="alert"]').text()).toBe(en.invalidJSON)
    expect(mocks.saveClientConfiguration).not.toHaveBeenCalled()
    const invalidDefault = { ...config, defaults: { chat: 'missing' } }
    await wrapper.get('textarea').setValue(JSON.stringify(invalidDefault))
    mocks.saveClientConfiguration.mockRejectedValueOnce({ message: 'Default must reference an available route' })
    await wrapper.get('[data-test="save-client-configuration"]').trigger('click')
    await flushPromises()
    expect(wrapper.get('[role="alert"]').text()).toContain('Default must reference')
    expect(JSON.parse(wrapper.get('textarea').element.value)).toEqual(invalidDefault)
    expect(wrapper.get('[data-test="save-client-configuration"]').attributes('disabled')).toBeUndefined()
  })

  it('guards duplicate publication while saving', async () => {
    let finish!: (value: unknown) => void
    mocks.saveClientConfiguration.mockImplementationOnce(() => new Promise(resolve => { finish = resolve }))
    const wrapper = mount(AutoDesignClientSettings)
    await flushPromises()
    await wrapper.get('[data-test="save-client-configuration"]').trigger('click')
    expect(wrapper.get('[data-test="save-client-configuration"]').attributes('disabled')).toBeDefined()
    expect(wrapper.get('textarea').attributes('disabled')).toBeDefined()
    await wrapper.get('[data-test="save-client-configuration"]').trigger('click')
    expect(mocks.saveClientConfiguration).toHaveBeenCalledTimes(1)
    finish(config)
    await flushPromises()
    expect(wrapper.get('[role="status"]').text()).toBe(en.saved)
  })
})
