<template>
  <div class="space-y-4">
    <div class="flex items-center justify-between">
      <p class="text-sm text-gray-500 dark:text-gray-400">
        按 webappId 添加 AI 应用，进入详情可替换默认图片/音频等参数，开启后客户端可见。
      </p>
      <button type="button" class="btn btn-primary shrink-0" @click="showAdd = true">
        <Icon name="plus" size="md" class="mr-2" />
        添加工作流
      </button>
    </div>

    <div class="overflow-hidden rounded-lg border border-gray-200 bg-white shadow-sm dark:border-dark-600 dark:bg-dark-800">
      <div v-if="loading && !workflows.length" class="p-10 text-center text-sm text-gray-500">加载中…</div>
      <div v-else-if="!workflows.length" class="p-10 text-center text-sm text-gray-500">还没有工作流。</div>
      <div v-else class="overflow-x-auto">
        <table class="w-full min-w-[720px] text-left text-sm">
          <thead class="border-b border-gray-200 bg-gray-50 text-xs uppercase text-gray-500 dark:border-dark-600 dark:bg-dark-900/40 dark:text-gray-400">
            <tr>
              <th class="px-5 py-3 font-medium">名称</th>
              <th class="px-4 py-3 font-medium">webappId</th>
              <th class="px-4 py-3 font-medium">预估 RH 币</th>
              <th class="px-4 py-3 font-medium">参数数</th>
              <th class="px-4 py-3 font-medium">客户端可见</th>
              <th class="px-5 py-3 text-right font-medium">操作</th>
            </tr>
          </thead>
          <tbody class="divide-y divide-gray-100 dark:divide-dark-600">
            <tr v-for="w in workflows" :key="w.id" class="hover:bg-gray-50/60 dark:hover:bg-dark-700/30">
              <td class="px-5 py-4 font-medium text-gray-900 dark:text-white">{{ w.name }}</td>
              <td class="px-4 py-4 font-mono text-xs text-gray-600 dark:text-gray-300">{{ w.webapp_id }}</td>
              <td class="px-4 py-4 tabular-nums text-gray-700 dark:text-gray-300">{{ w.estimated_rh }}</td>
              <td class="px-4 py-4 tabular-nums text-gray-700 dark:text-gray-300">{{ w.node_info_list.length }}</td>
              <td class="px-4 py-4">
                <button
                  type="button"
                  role="switch"
                  :aria-checked="w.enabled"
                  :aria-label="`${w.name} 客户端可见`"
                  class="relative inline-flex h-5 w-9 items-center rounded-full transition-colors"
                  :class="w.enabled ? 'bg-green-500' : 'bg-gray-300 dark:bg-dark-600'"
                  :disabled="busyId === w.id"
                  @click="toggle(w)"
                >
                  <span class="inline-block h-4 w-4 transform rounded-full bg-white transition" :class="w.enabled ? 'translate-x-4' : 'translate-x-0.5'" />
                </button>
              </td>
              <td class="space-x-1 px-5 py-4 text-right">
                <button type="button" class="rounded px-2 py-1 text-xs text-gray-600 hover:bg-gray-50 dark:text-gray-400 dark:hover:bg-dark-700" @click="openPreview(w)">
                  预览
                </button>
                <button type="button" class="rounded px-2 py-1 text-xs text-primary-600 hover:bg-primary-50 dark:hover:bg-primary-900/20" @click="openEdit(w)">
                  编辑
                </button>
                <button type="button" class="rounded px-2 py-1 text-xs text-red-600 hover:bg-red-50 dark:hover:bg-red-900/20" :disabled="busyId === w.id" @click="remove(w)">
                  删除
                </button>
              </td>
            </tr>
          </tbody>
        </table>
      </div>
    </div>

    <p v-if="error" class="rounded-md bg-red-50 px-4 py-3 text-sm text-red-700 dark:bg-red-900/20 dark:text-red-300" role="alert">
      {{ error }}
    </p>

    <!-- Add dialog -->
    <div v-if="showAdd" class="fixed inset-0 z-50 flex items-center justify-center bg-gray-900/50 p-4" @click.self="showAdd = false">
      <form class="w-full max-w-md rounded-xl bg-white p-6 shadow-2xl dark:bg-dark-800" @submit.prevent="handleAdd">
        <h2 class="mb-5 text-base font-semibold text-gray-900 dark:text-white">添加工作流</h2>
        <div class="space-y-4">
          <label class="block text-sm text-gray-700 dark:text-gray-300">
            webappId
            <input v-model.trim="addForm.webapp_id" required class="input mt-1 font-mono text-sm" placeholder="例如 2006270540499128322" />
          </label>
          <label class="block text-sm text-gray-700 dark:text-gray-300">
            名称
            <input v-model="addForm.name" class="input mt-1" placeholder="留空则使用 webappId" />
          </label>
          <label class="block text-sm text-gray-700 dark:text-gray-300">
            预估 RH 币（留空则从官方平均运行时长自动推算）
            <input v-model.number="addForm.estimated_rh" type="number" min="1" step="any" class="input mt-1" placeholder="自动推算" />
          </label>
          <label class="block text-sm text-gray-700 dark:text-gray-300">
            机型 instanceType
            <input v-model.trim="addForm.instance_type" class="input mt-1" placeholder="留空为默认；高分辨率建议 plus" />
          </label>
        </div>
        <p class="mt-3 text-xs text-gray-400">
          添加时会从 RunningHub 拉取参数列表和平均运行时长，按机型单价（Lite 0.02 / Standard 0.2 / Plus 0.4 / Ultra 0.6 币每秒）推算预估，默认不对客户端开放。
        </p>
        <p v-if="addError" class="mt-3 text-sm text-red-600 dark:text-red-400" role="alert">{{ addError }}</p>
        <div class="mt-6 flex justify-end gap-3">
          <button type="button" class="btn btn-secondary" @click="showAdd = false">取消</button>
          <button type="submit" class="btn btn-primary" :disabled="adding">{{ adding ? '拉取中…' : '添加' }}</button>
        </div>
      </form>
    </div>

    <RunningHubWorkflowEditor v-if="editing" :workflow="editing" :readonly="editMode === 'preview'" @close="editing = null" @saved="onSaved" />
  </div>
</template>

<script setup lang="ts">
import { onMounted, ref } from 'vue'
import Icon from '@/components/icons/Icon.vue'
import {
  createWorkflow,
  deleteWorkflow,
  listWorkflows,
  updateWorkflow,
  type RunningHubWorkflow,
} from '@/api/admin/runninghub'
import RunningHubWorkflowEditor from './RunningHubWorkflowEditor.vue'
import { errMsg } from './errors'

const workflows = ref<RunningHubWorkflow[]>([])
const loading = ref(false)
const error = ref('')
const busyId = ref<number | null>(null)
const editing = ref<RunningHubWorkflow | null>(null)
const editMode = ref<'preview' | 'edit'>('preview')

const showAdd = ref(false)
const adding = ref(false)
const addError = ref('')
// v-model.number leaves "" when the user clears a number input.
// We treat that as "not provided" so the backend can derive the value.
const optionalNumber = (v: string | number | undefined): number | undefined =>
  typeof v === 'number' && isFinite(v) && v > 0 ? v : undefined

// estimated_rh is string | number: v-model.number yields "" when the input is
// cleared, and empty means "let the backend derive it from avgRunningSeconds".
const emptyAdd = () => ({ webapp_id: '', name: '', estimated_rh: '' as string | number, instance_type: '' })
const addForm = ref(emptyAdd())

async function load() {
  loading.value = true
  error.value = ''
  try {
    workflows.value = await listWorkflows()
  } catch (e) {
    error.value = errMsg(e)
  } finally {
    loading.value = false
  }
}

async function handleAdd() {
  adding.value = true
  addError.value = ''
  try {
    const { estimated_rh, ...rest } = addForm.value
    const est = optionalNumber(estimated_rh)
    await createWorkflow(est && est > 0 ? { ...rest, estimated_rh: est } : rest)
    showAdd.value = false
    addForm.value = emptyAdd()
    await load()
  } catch (e) {
    addError.value = errMsg(e)
  } finally {
    adding.value = false
  }
}

async function toggle(w: RunningHubWorkflow) {
  busyId.value = w.id
  error.value = ''
  try {
    const updated = await updateWorkflow(w.id, { enabled: !w.enabled })
    Object.assign(w, updated)
  } catch (e) {
    error.value = errMsg(e)
  } finally {
    busyId.value = null
  }
}

async function remove(w: RunningHubWorkflow) {
  if (!confirm(`确定删除工作流「${w.name}」？客户端将无法再提交该应用。`)) return
  busyId.value = w.id
  error.value = ''
  try {
    await deleteWorkflow(w.id)
    await load()
  } catch (e) {
    error.value = errMsg(e)
  } finally {
    busyId.value = null
  }
}

function openPreview(w: RunningHubWorkflow) {
  editing.value = w
  editMode.value = 'preview'
}

function openEdit(w: RunningHubWorkflow) {
  editing.value = w
  editMode.value = 'edit'
}

function onSaved(w: RunningHubWorkflow) {
  const i = workflows.value.findIndex((x) => x.id === w.id)
  if (i >= 0) workflows.value[i] = w
  editing.value = null
}

onMounted(load)
</script>
