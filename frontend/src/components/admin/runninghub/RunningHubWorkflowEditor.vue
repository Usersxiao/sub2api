<template>
  <div class="fixed inset-0 z-50 flex items-center justify-center bg-gray-900/50 p-4" @click.self="$emit('close')">
    <form
      class="flex max-h-[90vh] w-full max-w-3xl flex-col rounded-xl bg-white shadow-2xl dark:bg-dark-800"
      aria-labelledby="rh-wf-title"
      @submit.prevent="!readonly && save()"
    >
      <div class="flex items-center justify-between border-b border-gray-100 px-6 py-4 dark:border-dark-600">
        <div>
          <h2 id="rh-wf-title" class="text-base font-semibold text-gray-900 dark:text-white">{{ form.name }}</h2>
          <p class="font-mono text-xs text-gray-400">{{ workflow.webapp_id }}</p>
        </div>
        <button v-if="!readonly" type="button" class="btn btn-secondary btn-sm" :disabled="refreshing" @click="refresh">
          <Icon name="refresh" size="sm" class="mr-1" :class="refreshing ? 'animate-spin' : ''" />
          重新拉取参数
        </button>
      </div>

      <div class="flex-1 space-y-5 overflow-y-auto px-6 py-5">
        <div class="grid gap-4 sm:grid-cols-2">
          <label class="block text-sm text-gray-700 dark:text-gray-300">
            名称
            <input v-model="form.name" required class="input mt-1" :disabled="readonly" />
          </label>
          <label class="block text-sm text-gray-700 dark:text-gray-300">
            排序（小的在前）
            <input v-model.number="form.sort_order" type="number" class="input mt-1" :disabled="readonly" />
          </label>
          <label class="block text-sm text-gray-700 dark:text-gray-300">
            预估 RH 币
            <input v-model.number="form.estimated_rh" type="number" min="1" step="any" required class="input mt-1" :disabled="readonly" />
            <span v-if="props.workflow.avg_running_seconds > 0" class="mt-1 block text-xs text-gray-400">
              官方平均运行 {{ props.workflow.avg_running_seconds }} 秒 ×
              {{ coinRate }} 币/秒（{{ rateTier }}）≈ {{ officialEstimate }} RH 币
            </span>
          </label>
          <label class="block text-sm text-gray-700 dark:text-gray-300">
            机型 instanceType
            <select v-model="form.instance_type" class="input mt-1" :disabled="readonly">
              <option value="">默认（Standard）</option>
              <option value="lite">Lite — 0.02 币/秒</option>
              <option value="standard">Standard — 0.2 币/秒</option>
              <option value="plus">Plus — 0.4 币/秒</option>
              <option value="ultra">Ultra — 0.6 币/秒</option>
            </select>
          </label>
          <label class="block text-sm text-gray-700 sm:col-span-2 dark:text-gray-300">
            描述（客户端展示）
            <textarea v-model="form.description" rows="2" class="input mt-1" :disabled="readonly" />
          </label>
          <label class="block text-sm text-gray-700 dark:text-gray-300">
            分类标签（客户端分类筛选用）
            <input v-model="form.category" class="input mt-1" :disabled="readonly" placeholder="如：视频、音频、图像（留空不显示分类）" />
          </label>
        </div>

        <div>
          <h3 class="mb-2 text-sm font-medium text-gray-900 dark:text-white">默认参数</h3>
          <p class="mb-3 text-xs text-gray-400">
            客户端提交时只能覆盖这里列出的字段；未覆盖的字段使用此处默认值。文件类参数可上传替换。
          </p>
          <div class="divide-y divide-gray-100 rounded-lg border border-gray-200 dark:divide-dark-600 dark:border-dark-600">
            <div v-for="(n, i) in nodes" :key="`${n.nodeId}/${n.fieldName}`" class="grid gap-2 p-3 sm:grid-cols-[200px_1fr]">
              <div class="text-xs">
                <p class="font-medium text-gray-800 dark:text-gray-200">{{ n.description || n.nodeName || n.fieldName }}</p>
                <p class="font-mono text-gray-400">#{{ n.nodeId }} · {{ n.fieldName }}<span v-if="n.fieldType"> · {{ n.fieldType }}</span></p>
              </div>
              <div class="flex flex-col gap-2">
                <div class="flex items-center gap-2">
                  <input
                    :value="displayValue(n.fieldValue)"
                    class="input flex-1 font-mono text-xs"
                    :aria-label="`${n.description || n.fieldName} 默认值`"
                    :disabled="readonly"
                    @input="!readonly && (nodes[i].fieldValue = ($event.target as HTMLInputElement).value)"
                  />
                  <label
                    v-if="isFileField(n) && !readonly"
                    class="btn btn-secondary btn-sm cursor-pointer shrink-0"
                    :class="uploadingIdx === i ? 'pointer-events-none opacity-60' : ''"
                  >
                    <Icon name="upload" size="sm" class="mr-1" />
                    {{ uploadingIdx === i ? '上传中' : '替换' }}
                    <input type="file" class="sr-only" :accept="acceptFor(n)" @change="onFile(i, $event)" />
                  </label>
                </div>
                <!-- media preview -->
                <!-- :key forces element recreation when the source URL changes so the
                     browser actually loads the new file (media elements don't reload
                     automatically when only the src attribute is mutated in-place). -->
                <template v-if="isFileField(n) && mediaPreviewUrl(n)">
                  <img
                    v-if="mediaTypeFor(n) === 'image'"
                    :key="mediaPreviewUrl(n) ?? undefined"
                    :src="mediaPreviewUrl(n)!"
                    :alt="`${n.description || n.fieldName} 预览`"
                    class="max-h-48 max-w-full rounded-md border border-gray-200 object-contain dark:border-dark-600"
                  />
                  <video
                    v-else-if="mediaTypeFor(n) === 'video'"
                    :key="mediaPreviewUrl(n) ?? undefined"
                    :src="mediaPreviewUrl(n)!"
                    controls
                    class="max-h-48 max-w-full rounded-md border border-gray-200 dark:border-dark-600"
                    :aria-label="`${n.description || n.fieldName} 预览`"
                  />
                  <audio
                    v-else-if="mediaTypeFor(n) === 'audio'"
                    :key="mediaPreviewUrl(n) ?? undefined"
                    :src="mediaPreviewUrl(n)!"
                    controls
                    preload="metadata"
                    class="w-full"
                    :aria-label="`${n.description || n.fieldName} 预览`"
                  />
                </template>
              </div>
            </div>
            <p v-if="!nodes.length" class="p-4 text-center text-xs text-gray-400">该应用没有可配置参数。</p>
          </div>
        </div>
      </div>

      <div class="flex items-center justify-between gap-3 border-t border-gray-100 px-6 py-4 dark:border-dark-600">
        <label v-if="!readonly" class="flex items-center gap-2 text-sm text-gray-700 dark:text-gray-300">
          <input v-model="form.enabled" type="checkbox" class="h-4 w-4 rounded" />
          对客户端开放
        </label>
        <div class="flex items-center gap-3" :class="readonly ? 'ml-auto' : ''">
          <p v-if="error" class="text-sm text-red-600 dark:text-red-400" role="alert">{{ error }}</p>
          <button type="button" class="btn btn-secondary" @click="$emit('close')">{{ readonly ? '关闭' : '取消' }}</button>
          <button v-if="!readonly" type="submit" class="btn btn-primary" :disabled="saving">{{ saving ? '保存中…' : '保存' }}</button>
        </div>
      </div>
    </form>
  </div>
</template>

<script setup lang="ts">
import { computed, onBeforeUnmount, reactive, ref } from 'vue'
import Icon from '@/components/icons/Icon.vue'
import {
  getMediaFile,
  refreshWorkflow,
  updateWorkflow,
  uploadMedia,
  type RunningHubNode,
  type RunningHubWorkflow,
} from '@/api/admin/runninghub'
import { errMsg } from './errors'

const props = defineProps<{ workflow: RunningHubWorkflow; readonly?: boolean }>()
const emit = defineEmits<{ close: []; saved: [w: RunningHubWorkflow] }>()

const clone = <T,>(v: T): T => JSON.parse(JSON.stringify(v))

// v-model.number leaves "" when the user clears a number input; map that to
// undefined so the server PATCH omits the field rather than sending a string.
const optionalNumber = (v: string | number | undefined): number | undefined =>
  typeof v === 'number' && isFinite(v) ? v : undefined

const form = ref({
  name: props.workflow.name,
  description: props.workflow.description,
  instance_type: props.workflow.instance_type,
  estimated_rh: props.workflow.estimated_rh,
  sort_order: props.workflow.sort_order,
  enabled: props.workflow.enabled,
  category: (props.workflow as any).category ?? '',
})
// RunningHub's published per-second coin rates (官方计费说明). The official UI
// derives its "预估 ≈ N RH币" from avgRunningSeconds × the selected tier's rate.
const COIN_RATES: Record<string, number> = { lite: 0.02, standard: 0.2, plus: 0.4, ultra: 0.6 }
const DEFAULT_TIER = 'standard'

const rateTier = computed(() => {
  const t = form.value.instance_type.trim().toLowerCase()
  return t in COIN_RATES ? t : DEFAULT_TIER
})
const coinRate = computed(() => COIN_RATES[rateTier.value])
const officialEstimate = computed(() =>
  Math.round(props.workflow.avg_running_seconds * coinRate.value * 100) / 100,
)

const nodes = ref<RunningHubNode[]>(clone(props.workflow.node_info_list))
const saving = ref(false)
const refreshing = ref(false)
const uploadingIdx = ref<number | null>(null)
const error = ref('')

const fileTypes = ['IMAGE', 'AUDIO', 'VIDEO']

// Returns the media kind ('image' | 'video' | 'audio') for a node, or null.
// Tries fieldType first (lenient substring match to handle RunningHub variants
// like "IMAGE_UPLOAD", "videoInput"), then falls back to the file extension
// embedded in fieldValue when the value is a staged media ref.
function mediaTypeFor(n: RunningHubNode): 'image' | 'video' | 'audio' | null {
  const ft = String(n.fieldType ?? '').toUpperCase()
  if (ft.includes('IMAGE')) return 'image'
  if (ft.includes('VIDEO')) return 'video'
  if (ft.includes('AUDIO')) return 'audio'
  return null
}

function isFileField(n: RunningHubNode) {
  if (fileTypes.some(t => String(n.fieldType ?? '').toUpperCase().includes(t))) return true
  // A node whose value is already a staged media ref is a file field even
  // when RunningHub didn't return a recognised fieldType for it.
  return typeof n.fieldValue === 'string' && n.fieldValue.startsWith('media:')
}

// Maps a file extension or MIME type to a media kind.
function kindFromFile(name: string, mimeType: string): 'image' | 'video' | 'audio' | undefined {
  const ext = name.split('.').pop()?.toLowerCase() ?? ''
  const mime = mimeType.toLowerCase()
  if (mime.startsWith('image/') || ['jpg','jpeg','png','webp','gif','bmp','svg','tiff'].includes(ext)) return 'image'
  if (mime.startsWith('audio/') || ['mp3','wav','m4a','flac','ogg','aac','opus'].includes(ext)) return 'audio'
  if (mime.startsWith('video/') || ['mp4','mov','webm','mkv','avi','wmv','flv'].includes(ext)) return 'video'
  return undefined
}

// Returns a renderable URL for the node's current value, or null.
// - media:<uuid>  → blob URL of the locally-staged file. The staging endpoint is
//   admin-only and <img>/<video>/<audio> can't send the Authorization header,
//   so the file is fetched through apiClient and played from a blob.
// - https:// URL  → rendered directly
// - RH internal filename → RH's public /view endpoint
function mediaPreviewUrl(n: RunningHubNode): string | null {
  const v = displayValue(n.fieldValue)
  if (!v || v === '{}') return null
  if (v.startsWith('media:')) {
    const id = v.slice('media:'.length)
    // Deferred so render never mutates reactive state.
    if (!(id in blobUrls)) queueMicrotask(() => loadMediaBlob(id))
    return blobUrls[id] || null
  }
  if (v.startsWith('http://') || v.startsWith('https://')) return v
  return `https://www.runninghub.cn/view?filename=${encodeURIComponent(v)}&subfolder=&type=input`
}

// media id → object URL ('' while loading or after a failed fetch).
const blobUrls = reactive<Record<string, string>>({})

async function loadMediaBlob(id: string) {
  if (id in blobUrls) return
  blobUrls[id] = ''
  try {
    const blob = await getMediaFile(id)
    blobUrls[id] = URL.createObjectURL(blob)
  } catch (e) {
    error.value = errMsg(e)
  }
}

onBeforeUnmount(() => {
  Object.values(blobUrls).forEach((u) => u && URL.revokeObjectURL(u))
})

function acceptFor(n: RunningHubNode) {
  const t = String(n.fieldType ?? '').toLowerCase()
  return `${t}/*`
}

function displayValue(v: unknown) {
  return typeof v === 'string' ? v : JSON.stringify(v ?? '')
}

async function onFile(i: number, ev: Event) {
  const input = ev.target as HTMLInputElement
  const file = input.files?.[0]
  input.value = ''
  if (!file) return
  uploadingIdx.value = i
  error.value = ''
  try {
    const m = await uploadMedia(file)
    blobUrls[m.id] = URL.createObjectURL(file)
    // Determine media kind from backend then browser MIME, then filename extension.
    const kind = kindFromFile(file.name, file.type) ?? (['image','audio','video'] as const).find(k => m.file_type === k)
    // Replace the whole element so Vue's reactivity detects the change reliably.
    // In-place property mutation on a ref-wrapped array item can silently miss
    // the re-render when multiple properties change in the same tick.
    nodes.value[i] = {
      ...nodes.value[i],
      fieldValue: m.ref,
      ...(kind ? { fieldType: kind.toUpperCase() } : {}),
    }
  } catch (e) {
    error.value = errMsg(e)
  } finally {
    uploadingIdx.value = null
  }
}

async function refresh() {
  refreshing.value = true
  error.value = ''
  try {
    const w = await refreshWorkflow(props.workflow.id)
    nodes.value = clone(w.node_info_list)
  } catch (e) {
    error.value = errMsg(e)
  } finally {
    refreshing.value = false
  }
}

async function save() {
  saving.value = true
  error.value = ''
  try {
    // v-model.number leaves "" in a cleared input; the backend needs numbers.
    const { estimated_rh, sort_order, ...rest } = form.value
    const w = await updateWorkflow(props.workflow.id, {
      ...rest,
      estimated_rh: optionalNumber(estimated_rh),
      sort_order: optionalNumber(sort_order),
      node_info_list: nodes.value,
    })
    emit('saved', w)
  } catch (e) {
    error.value = errMsg(e)
  } finally {
    saving.value = false
  }
}
</script>
