<template>
  <section class="card space-y-4 p-6" :aria-label="t('clientConfiguration.title')">
    <div>
      <h2 class="text-lg font-semibold text-gray-900 dark:text-white">{{ t('clientConfiguration.title') }}</h2>
      <p class="mt-1 text-sm text-gray-500 dark:text-gray-400">{{ t('clientConfiguration.description') }}</p>
    </div>
    <p class="rounded-lg bg-amber-50 p-3 text-sm text-amber-800 dark:bg-amber-900/20 dark:text-amber-200">{{ t('clientConfiguration.readOnly') }}</p>
    <p v-if="error" role="alert" class="break-words text-sm text-red-600 dark:text-red-400">{{ error }}</p>
    <p v-if="notice" role="status" class="text-sm text-green-700 dark:text-green-400">{{ notice }}</p>
    <p v-if="loading" role="status" class="text-sm text-gray-500">{{ t('clientConfiguration.loading') }}</p>
    <div class="flex flex-wrap gap-2">
      <button type="button" data-test="save-client-configuration" class="btn btn-primary" :disabled="busy || loading || !loaded" @click="save">{{ busy ? t('clientConfiguration.saving') : t('clientConfiguration.save') }}</button>
      <button type="button" data-test="reload-client-configuration" class="btn btn-secondary" :disabled="busy || loading" @click="load">{{ t('clientConfiguration.reload') }}</button>
    </div>
    <template v-if="loaded">
      <p class="text-sm text-gray-500 dark:text-gray-400">{{ routeCount ? t('clientConfiguration.routeCount', { count: routeCount }) : t('clientConfiguration.empty') }}</p>
      <label for="autodesign-client-configuration-json" class="block text-sm font-medium text-gray-700 dark:text-gray-300">{{ t('clientConfiguration.editor') }}</label>
      <textarea
        id="autodesign-client-configuration-json"
        v-model="draft"
        data-test="configuration-json"
        class="input min-h-48 w-full font-mono text-sm"
        rows="8"
        spellcheck="false"
        :disabled="busy || loading"
        :aria-describedby="'autodesign-client-configuration-help'"
        @input="notice = ''"
      />
      <p id="autodesign-client-configuration-help" class="text-sm text-gray-500 dark:text-gray-400">{{ t('clientConfiguration.help') }}</p>
      <details class="rounded-lg border border-gray-200 p-3 dark:border-dark-700">
        <summary class="cursor-pointer text-sm font-medium">{{ t('clientConfiguration.example') }}</summary>
        <p class="my-3 text-sm text-gray-500 dark:text-gray-400">{{ t('clientConfiguration.exampleHint') }}</p>
        <pre class="overflow-x-auto whitespace-pre text-xs">{{ example }}</pre>
      </details>
    </template>
  </section>
</template>

<script setup lang="ts">
import { onMounted, ref } from 'vue'
import { useI18n } from 'vue-i18n'
import { getClientConfiguration, saveClientConfiguration } from '@/api/admin/clientConfiguration'
import { extractApiErrorMessage } from '@/utils/apiError'

const { t } = useI18n()
const draft = ref('')
const loading = ref(false)
const loaded = ref(false)
const busy = ref(false)
const routeCount = ref(0)
const error = ref('')
const notice = ref('')
const example = JSON.stringify({
  groupIds: [1],
  route: {
    routeId: 'writing', target: { kind: 'model', modelId: 'EXISTING_MODEL_ID' },
    displayName: 'Writing', providerDisplay: 'AI', purposes: ['chat', 'design'],
    operation: 'text', availability: { status: 'available' }, protocol: 'responses', transport: 'json',
    capabilities: { streaming: true, tools: false, structuredOutput: false, vision: false, uploads: false, asynchronous: false, cancellation: false },
    inputKinds: ['text'], outputKinds: ['text'],
    parameters: [{ name: 'temperature', label: 'Temperature', editable: true, type: 'number', default: 0.7, minimum: 0, maximum: 1, step: 0.1 }],
    limits: {}, prices: []
  }
}, null, 2)

async function load() {
  if (loading.value || busy.value) return
  loading.value = true
  error.value = ''
  notice.value = ''
  try {
    const config = await getClientConfiguration()
    draft.value = JSON.stringify(config, null, 2)
    routeCount.value = config.routes.length
    loaded.value = true
  } catch {
    error.value = t('clientConfiguration.loadError')
  } finally { loading.value = false }
}

async function save() {
  if (busy.value || loading.value || !loaded.value) return
  error.value = ''
  notice.value = ''
  let value: unknown
  try {
    value = JSON.parse(draft.value)
    if (!value || typeof value !== 'object' || Array.isArray(value)) throw new Error()
  } catch { error.value = t('clientConfiguration.invalidJSON'); return }
  if (new TextEncoder().encode(draft.value).byteLength > 1024 * 1024) { error.value = t('clientConfiguration.tooLarge'); return }
  busy.value = true
  try {
    const config = await saveClientConfiguration(value)
    draft.value = JSON.stringify(config, null, 2)
    routeCount.value = config.routes.length
    notice.value = t('clientConfiguration.saved')
  } catch (cause) {
    error.value = extractApiErrorMessage(cause, t('clientConfiguration.saveError'))
  } finally { busy.value = false }
}

onMounted(load)
</script>
