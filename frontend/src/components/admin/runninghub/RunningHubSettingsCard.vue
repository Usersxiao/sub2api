<template>
  <div class="rounded-lg border border-gray-200 bg-white p-5 dark:border-dark-600 dark:bg-dark-800">
    <div class="flex flex-col gap-4 lg:flex-row lg:items-end lg:justify-between">
      <div class="grid flex-1 gap-4 sm:grid-cols-3">
        <label class="flex items-start gap-3 text-sm text-gray-700 dark:text-gray-300">
          <input v-model="form.consumer_enabled" type="checkbox" class="mt-0.5 h-4 w-4 rounded" />
          <span>
            启用消费级账号
            <span class="block text-xs text-gray-400">优先调度，Max 会员按 RH 币结算</span>
          </span>
        </label>
        <label class="flex items-start gap-3 text-sm text-gray-700 dark:text-gray-300">
          <input v-model="form.enterprise_enabled" type="checkbox" class="mt-0.5 h-4 w-4 rounded" />
          <span>
            启用企业级账号
            <span class="block text-xs text-gray-400">两者都开时，消费级满并发自动溢出到企业级</span>
          </span>
        </label>
        <label class="block text-sm text-gray-700 dark:text-gray-300">
          企业级 元→RH 币换算
          <input
            v-model.number="form.yuan_to_coin"
            type="number"
            min="0.000001"
            step="any"
            class="input mt-1"
            aria-describedby="rh-yuan-hint"
          />
          <span id="rh-yuan-hint" class="mt-1 block text-xs text-gray-400">
            企业 key 返回 consumeMoney（元），乘以此系数后扣用户积分
          </span>
        </label>
      </div>
      <button type="button" class="btn btn-primary shrink-0" :disabled="saving || !dirty" @click="save">
        {{ saving ? '保存中…' : '保存设置' }}
      </button>
    </div>
    <p v-if="!form.consumer_enabled && !form.enterprise_enabled" class="mt-3 text-xs text-amber-600 dark:text-amber-400">
      两类账号都关闭时，所有任务提交都会被拒绝。
    </p>
    <p v-if="error" class="mt-3 text-sm text-red-600 dark:text-red-400" role="alert">{{ error }}</p>
  </div>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'
import { getSettings, updateSettings, type RunningHubSettings } from '@/api/admin/runninghub'
import { errMsg } from './errors'

const saved = ref<RunningHubSettings>({ consumer_enabled: true, enterprise_enabled: false, yuan_to_coin: 1 })
const form = ref<RunningHubSettings>({ ...saved.value })
const saving = ref(false)
const error = ref('')

const dirty = computed(() => JSON.stringify(form.value) !== JSON.stringify(saved.value))

async function load() {
  try {
    saved.value = await getSettings()
    form.value = { ...saved.value }
  } catch (e) {
    error.value = errMsg(e)
  }
}

async function save() {
  saving.value = true
  error.value = ''
  try {
    saved.value = await updateSettings(form.value)
    form.value = { ...saved.value }
  } catch (e) {
    error.value = errMsg(e)
  } finally {
    saving.value = false
  }
}

onMounted(load)
</script>
