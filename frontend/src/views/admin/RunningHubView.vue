<template>
  <AppLayout>
    <div class="space-y-6">
      <!-- Header -->
      <div class="flex flex-col gap-4 sm:flex-row sm:items-center sm:justify-between">
        <div>
          <h1 class="text-xl font-semibold text-gray-900 dark:text-white">RunningHub</h1>
          <p class="mt-1 text-sm text-gray-500 dark:text-gray-400">
            号池按负载轮询调度；某账号返回并发已满时立即切换下一个可用账号。
          </p>
        </div>
        <button v-if="tab === 'accounts'" type="button" class="btn btn-primary shrink-0" @click="showAdd = true">
          <Icon name="plus" size="md" class="mr-2" />
          添加账号
        </button>
      </div>

      <div class="flex gap-1 border-b border-gray-200 dark:border-dark-600" role="tablist">
        <button
          v-for="t in tabs"
          :key="t.key"
          type="button"
          role="tab"
          :aria-selected="tab === t.key"
          class="-mb-px border-b-2 px-4 py-2 text-sm font-medium transition-colors"
          :class="tab === t.key
            ? 'border-primary-500 text-primary-600 dark:text-primary-400'
            : 'border-transparent text-gray-500 hover:text-gray-700 dark:text-gray-400 dark:hover:text-gray-200'"
          @click="tab = t.key"
        >
          {{ t.label }}
        </button>
      </div>

      <RunningHubWorkflows v-if="tab === 'workflows'" />

      <template v-else>
      <RunningHubSettingsCard />

      <!-- Stats row -->
      <div class="grid grid-cols-2 gap-4 sm:grid-cols-4">
        <div class="rounded-lg border border-gray-200 bg-white p-4 dark:border-dark-600 dark:bg-dark-800">
          <p class="text-xs text-gray-500 dark:text-gray-400">账号总数</p>
          <p class="mt-1 text-2xl font-semibold tabular-nums text-gray-900 dark:text-white">{{ accounts.length }}</p>
        </div>
        <div class="rounded-lg border border-gray-200 bg-white p-4 dark:border-dark-600 dark:bg-dark-800">
          <p class="text-xs text-gray-500 dark:text-gray-400">已启用</p>
          <p class="mt-1 text-2xl font-semibold tabular-nums text-green-600 dark:text-green-400">{{ enabledCount }}</p>
        </div>
        <div class="rounded-lg border border-gray-200 bg-white p-4 dark:border-dark-600 dark:bg-dark-800">
          <p class="text-xs text-gray-500 dark:text-gray-400">总并发上限</p>
          <p class="mt-1 text-2xl font-semibold tabular-nums text-gray-900 dark:text-white">{{ totalConcurrency }}</p>
        </div>
        <div class="rounded-lg border border-gray-200 bg-white p-4 dark:border-dark-600 dark:bg-dark-800">
          <p class="text-xs text-gray-500 dark:text-gray-400">当前占用</p>
          <p class="mt-1 text-2xl font-semibold tabular-nums text-amber-600 dark:text-amber-400">{{ totalInFlight }}</p>
        </div>
      </div>

      <!-- Accounts table -->
      <div class="overflow-hidden rounded-lg border border-gray-200 bg-white shadow-sm dark:border-dark-600 dark:bg-dark-800">
        <div v-if="loading && !accounts.length" class="p-10 text-center text-sm text-gray-500">加载中…</div>
        <div v-else-if="!accounts.length" class="p-10 text-center text-sm text-gray-500">
          还没有账号，点击右上角添加第一个。
        </div>
        <div v-else class="overflow-x-auto">
          <table class="w-full min-w-[640px] text-left text-sm">
            <thead class="border-b border-gray-200 bg-gray-50 text-xs uppercase text-gray-500 dark:border-dark-600 dark:bg-dark-900/40 dark:text-gray-400">
              <tr>
                <th class="px-5 py-3 font-medium">名称</th>
                <th class="px-4 py-3 font-medium">类型</th>
                <th class="px-4 py-3 font-medium">并发上限</th>
                <th class="px-4 py-3 font-medium">当前占用</th>
                <th class="px-4 py-3 font-medium">状态</th>
                <th class="px-5 py-3 text-right font-medium">操作</th>
              </tr>
            </thead>
            <tbody class="divide-y divide-gray-100 dark:divide-dark-600">
              <tr v-for="a in accounts" :key="a.id" class="transition-colors hover:bg-gray-50/60 dark:hover:bg-dark-700/30">
                <td class="px-5 py-4 font-medium text-gray-900 dark:text-white">{{ a.name }}</td>
                <td class="px-4 py-4">
                  <span
                    class="inline-flex items-center rounded-full px-2.5 py-0.5 text-xs font-medium"
                    :class="a.tier === 'consumer'
                      ? 'bg-blue-50 text-blue-700 dark:bg-blue-900/20 dark:text-blue-300'
                      : 'bg-purple-50 text-purple-700 dark:bg-purple-900/20 dark:text-purple-300'"
                  >
                    {{ a.tier === 'consumer' ? '消费级' : '企业级' }}
                  </span>
                </td>
                <td class="px-4 py-4 tabular-nums text-gray-700 dark:text-gray-300">{{ a.max_concurrency }}</td>
                <td class="px-4 py-4">
                  <div class="flex items-center gap-2">
                    <div class="h-1.5 w-20 overflow-hidden rounded-full bg-gray-200 dark:bg-dark-600">
                      <div
                        class="h-full rounded-full transition-all duration-200"
                        :class="a.in_flight >= a.max_concurrency
                          ? 'bg-red-500'
                          : a.in_flight > 0
                            ? 'bg-amber-400'
                            : 'bg-green-400'"
                        :style="{ width: `${Math.min(100, a.max_concurrency > 0 ? (a.in_flight / a.max_concurrency) * 100 : 0)}%` }"
                      />
                    </div>
                    <span class="tabular-nums text-xs text-gray-500 dark:text-gray-400">{{ a.in_flight }}/{{ a.max_concurrency }}</span>
                  </div>
                </td>
                <td class="px-4 py-4">
                  <span
                    class="inline-flex items-center gap-1.5 text-xs font-medium"
                    :class="a.enabled ? 'text-green-600 dark:text-green-400' : 'text-gray-400 dark:text-gray-500'"
                  >
                    <span class="h-1.5 w-1.5 rounded-full" :class="a.enabled ? 'bg-green-500' : 'bg-gray-300 dark:bg-gray-600'" />
                    {{ a.enabled ? '启用' : '停用' }}
                  </span>
                </td>
                <td class="px-5 py-4 text-right">
                  <div class="flex items-center justify-end gap-2">
                    <button
                      type="button"
                      class="inline-flex items-center gap-1 rounded px-2 py-1 text-xs text-gray-600 hover:bg-gray-100 dark:text-gray-300 dark:hover:bg-dark-700"
                      @click="openEdit(a)"
                    >
                      <Icon name="edit" size="sm" />
                      编辑
                    </button>
                    <button
                      v-if="a.enabled"
                      type="button"
                      class="inline-flex items-center gap-1 rounded px-2 py-1 text-xs text-red-600 hover:bg-red-50 dark:hover:bg-red-900/20"
                      :disabled="deletingId === a.id"
                      @click="handleDelete(a)"
                    >
                      <Icon name="trash" size="sm" />
                      {{ deletingId === a.id ? '停用中' : '停用' }}
                    </button>
                    <button
                      v-else
                      type="button"
                      class="inline-flex items-center gap-1 rounded px-2 py-1 text-xs text-green-600 hover:bg-green-50 dark:hover:bg-green-900/20"
                      :disabled="deletingId === a.id"
                      @click="handleEnable(a)"
                    >
                      {{ deletingId === a.id ? '启用中' : '重新启用' }}
                    </button>
                  </div>
                </td>
              </tr>
            </tbody>
          </table>
        </div>
        <div class="flex items-center justify-between border-t border-gray-100 px-5 py-3 text-xs text-gray-500 dark:border-dark-600 dark:text-gray-400">
          <span>共 {{ accounts.length }} 个账号</span>
          <button
            type="button"
            class="flex items-center gap-1 hover:text-gray-700 dark:hover:text-gray-200"
            :disabled="loading"
            @click="load"
          >
            <Icon name="refresh" size="sm" :class="loading ? 'animate-spin' : ''" />
            刷新
          </button>
        </div>
      </div>

      <!-- Error banner -->
      <p v-if="error" class="rounded-md bg-red-50 px-4 py-3 text-sm text-red-700 dark:bg-red-900/20 dark:text-red-300" role="alert">
        {{ error }}
      </p>
      </template>
    </div>

    <!-- Add account dialog -->
    <div
      v-if="showAdd"
      class="fixed inset-0 z-50 flex items-center justify-center bg-gray-900/50 p-4"
      @click.self="showAdd = false"
    >
      <form
        class="w-full max-w-md rounded-xl bg-white p-6 shadow-2xl dark:bg-dark-800"
        @submit.prevent="handleAdd"
      >
        <h2 class="mb-5 text-base font-semibold text-gray-900 dark:text-white">添加 RunningHub 账号</h2>

        <div class="space-y-4">
          <label class="block text-sm text-gray-700 dark:text-gray-300">
            名称
            <input
              v-model="form.name"
              required
              class="input mt-1"
              placeholder="例如：rh-main-01"
            />
          </label>

          <label class="block text-sm text-gray-700 dark:text-gray-300">
            账号类型
            <select v-model="form.tier" class="input mt-1">
              <option value="consumer">消费级（Max 会员）</option>
              <option value="enterprise">企业级</option>
            </select>
          </label>

          <label class="block text-sm text-gray-700 dark:text-gray-300">
            API Key
            <input
              v-model="form.api_key"
              required
              type="password"
              class="input mt-1 font-mono text-sm"
              placeholder="RunningHub API Key"
              autocomplete="off"
            />
          </label>

          <label class="block text-sm text-gray-700 dark:text-gray-300">
            最大并发数
            <input
              v-model.number="form.max_concurrency"
              type="number"
              min="1"
              max="100"
              required
              class="input mt-1"
            />
            <span class="mt-1 block text-xs text-gray-400">
              Max 会员通常填 20；基础版通常填 2
            </span>
          </label>
        </div>

        <p v-if="addError" class="mt-3 text-sm text-red-600 dark:text-red-400">{{ addError }}</p>

        <div class="mt-6 flex justify-end gap-3">
          <button type="button" class="btn btn-secondary" @click="showAdd = false">取消</button>
          <button type="submit" class="btn btn-primary" :disabled="adding">
            {{ adding ? '添加中…' : '添加' }}
          </button>
        </div>
      </form>
    </div>

    <!-- Edit account dialog -->
    <div
      v-if="showEdit"
      class="fixed inset-0 z-50 flex items-center justify-center bg-gray-900/50 p-4"
      @click.self="showEdit = false"
    >
      <form
        class="w-full max-w-md rounded-xl bg-white p-6 shadow-2xl dark:bg-dark-800"
        @submit.prevent="handleEdit"
      >
        <h2 class="mb-5 text-base font-semibold text-gray-900 dark:text-white">编辑账号</h2>

        <div class="space-y-4">
          <label class="block text-sm text-gray-700 dark:text-gray-300">
            名称
            <input
              v-model="editForm.name"
              required
              class="input mt-1"
              placeholder="例如：rh-main-01"
            />
          </label>

          <label class="block text-sm text-gray-700 dark:text-gray-300">
            API Key
            <input
              v-model="editForm.api_key"
              type="password"
              class="input mt-1 font-mono text-sm"
              placeholder="留空则不修改"
              autocomplete="off"
            />
            <span class="mt-1 block text-xs text-gray-400">
              留空表示不修改 API Key
            </span>
          </label>

          <label class="block text-sm text-gray-700 dark:text-gray-300">
            最大并发数
            <input
              v-model.number="editForm.max_concurrency"
              type="number"
              min="1"
              max="100"
              required
              class="input mt-1"
            />
          </label>
        </div>

        <p v-if="editError" class="mt-3 text-sm text-red-600 dark:text-red-400">{{ editError }}</p>

        <div class="mt-6 flex justify-end gap-3">
          <button type="button" class="btn btn-secondary" @click="showEdit = false">取消</button>
          <button type="submit" class="btn btn-primary" :disabled="editing">
            {{ editing ? '保存中…' : '保存' }}
          </button>
        </div>
      </form>
    </div>
  </AppLayout>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'
import AppLayout from '@/components/layout/AppLayout.vue'
import Icon from '@/components/icons/Icon.vue'
import RunningHubSettingsCard from '@/components/admin/runninghub/RunningHubSettingsCard.vue'
import RunningHubWorkflows from '@/components/admin/runninghub/RunningHubWorkflows.vue'
import { errMsg } from '@/components/admin/runninghub/errors'
import {
  listAccounts,
  addAccount,
  deleteAccount,
  updateAccount,
  type RunningHubAccount,
} from '@/api/admin/runninghub'

const tabs = [
  { key: 'accounts', label: '账号池' },
  { key: 'workflows', label: '工作流' },
] as const
const tab = ref<(typeof tabs)[number]['key']>('accounts')

const accounts = ref<RunningHubAccount[]>([])
const loading = ref(false)
const error = ref('')
const deletingId = ref<number | null>(null)

const showAdd = ref(false)
const adding = ref(false)
const addError = ref('')
const form = ref({
  name: '',
  tier: 'consumer' as 'consumer' | 'enterprise',
  api_key: '',
  max_concurrency: 20,
})

const showEdit = ref(false)
const editing = ref(false)
const editError = ref('')
const editForm = ref({
  id: 0,
  name: '',
  api_key: '',
  max_concurrency: 20,
})

const enabledCount = computed(() => accounts.value.filter((a) => a.enabled).length)
const totalConcurrency = computed(() =>
  accounts.value.filter((a) => a.enabled).reduce((s, a) => s + a.max_concurrency, 0),
)
const totalInFlight = computed(() => accounts.value.reduce((s, a) => s + a.in_flight, 0))

async function load() {
  loading.value = true
  error.value = ''
  try {
    accounts.value = await listAccounts()
  } catch (e: unknown) {
    error.value = errMsg(e)
  } finally {
    loading.value = false
  }
}

async function handleAdd() {
  adding.value = true
  addError.value = ''
  try {
    await addAccount(form.value)
    showAdd.value = false
    form.value = { name: '', tier: 'consumer', api_key: '', max_concurrency: 20 }
    await load()
  } catch (e: unknown) {
    addError.value = errMsg(e)
  } finally {
    adding.value = false
  }
}

async function handleDelete(a: RunningHubAccount) {
  if (!confirm(`确定停用账号「${a.name}」？\n（历史任务记录保留，账号不会被物理删除）`)) return
  deletingId.value = a.id
  error.value = ''
  try {
    await deleteAccount(a.id)
    await load()
  } catch (e: unknown) {
    error.value = errMsg(e)
  } finally {
    deletingId.value = null
  }
}

async function handleEnable(a: RunningHubAccount) {
  deletingId.value = a.id
  error.value = ''
  try {
    await updateAccount(a.id, { enabled: true })
    await load()
  } catch (e: unknown) {
    error.value = errMsg(e)
  } finally {
    deletingId.value = null
  }
}

function openEdit(a: RunningHubAccount) {
  editForm.value = {
    id: a.id,
    name: a.name,
    api_key: '',
    max_concurrency: a.max_concurrency,
  }
  showEdit.value = true
}

async function handleEdit() {
  editing.value = true
  editError.value = ''
  try {
    const payload: { name: string; max_concurrency: number; api_key?: string } = {
      name: editForm.value.name,
      max_concurrency: editForm.value.max_concurrency,
    }
    if (editForm.value.api_key.trim()) {
      payload.api_key = editForm.value.api_key
    }
    await updateAccount(editForm.value.id, payload)
    showEdit.value = false
    editForm.value = { id: 0, name: '', api_key: '', max_concurrency: 20 }
    await load()
  } catch (e: unknown) {
    editError.value = errMsg(e)
  } finally {
    editing.value = false
  }
}

onMounted(load)
</script>
