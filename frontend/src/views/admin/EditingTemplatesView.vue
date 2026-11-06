<template>
  <AppLayout>
    <div class="space-y-6">
      <section class="flex flex-wrap items-center justify-between gap-4">
        <div>
          <h1 class="text-xl font-semibold tracking-tight text-gray-950 dark:text-white">
            {{ t('admin.editingTemplates.title') }}
          </h1>
          <p class="mt-1.5 text-sm text-gray-500 dark:text-gray-400">
            {{ t('admin.editingTemplates.queueHint') }}
          </p>
        </div>
        <button type="button" class="btn btn-secondary" :disabled="loading" @click="loadTemplates">
          <Icon name="refresh" size="sm" />
          {{ t('admin.editingTemplates.refresh') }}
        </button>
      </section>

      <section class="rounded-2xl border border-gray-200 bg-white p-4 shadow-sm dark:border-dark-700 dark:bg-dark-800 sm:p-5">
        <div class="flex flex-col gap-4 xl:flex-row xl:items-center xl:justify-between">
          <div class="flex min-w-0 flex-1 flex-col gap-3 sm:flex-row sm:items-center">
            <div class="relative min-w-0 flex-1 sm:max-w-md">
              <Icon name="search" size="sm" class="pointer-events-none absolute left-3 top-1/2 -translate-y-1/2 text-gray-400" />
              <input
                v-model.trim="search"
                class="input pl-9"
                type="search"
                :placeholder="t('admin.editingTemplates.searchPlaceholder')"
                @keyup.enter="applyFilters"
              />
            </div>
            <div class="flex max-w-full gap-1 overflow-x-auto rounded-xl bg-gray-100 p-1 dark:bg-dark-900/70">
              <button
                v-for="option in statusOptions"
                :key="option.value || 'all'"
                type="button"
                :aria-pressed="status === option.value"
                class="whitespace-nowrap rounded-lg px-3 py-1.5 text-xs font-medium transition"
                :class="status === option.value
                  ? 'bg-white text-gray-900 shadow-sm dark:bg-dark-700 dark:text-white'
                  : 'text-gray-500 hover:text-gray-800 dark:text-gray-400 dark:hover:text-gray-200'"
                @click="selectStatus(option.value)"
              >
                {{ option.label }}
              </button>
            </div>
          </div>
          <div class="text-sm text-gray-500 dark:text-gray-400">
            {{ t('admin.editingTemplates.pageSummary', { total: pagination.total }) }}
          </div>
        </div>
      </section>

      <div v-if="loading" class="template-review-grid grid gap-3">
        <div v-for="index in 6" :key="index" class="h-72 animate-pulse rounded-xl border border-gray-200 bg-gray-100 dark:border-dark-700 dark:bg-dark-800" />
      </div>

      <section
        v-else-if="templates.length === 0"
        class="flex min-h-72 flex-col items-center justify-center rounded-2xl border border-dashed border-gray-300 bg-white px-6 text-center dark:border-dark-600 dark:bg-dark-800"
      >
        <div class="flex h-12 w-12 items-center justify-center rounded-2xl bg-gray-100 text-gray-400 dark:bg-dark-700">
          <Icon name="cube" size="lg" />
        </div>
        <p class="mt-4 text-sm font-medium text-gray-700 dark:text-gray-200">{{ t('admin.editingTemplates.empty') }}</p>
      </section>

      <section v-else class="template-review-grid grid gap-3" data-testid="editing-template-review-grid">
        <article
          v-for="template in templates"
          :key="template.id"
          class="min-w-0 overflow-hidden rounded-xl border border-gray-200 bg-white dark:border-dark-700 dark:bg-dark-800"
        >
          <div class="flex h-full flex-col">
            <button
              type="button"
              :aria-label="`${t('admin.editingTemplates.view')}: ${template.title}`"
              class="relative block aspect-video w-full overflow-hidden bg-gray-100 focus-visible:outline focus-visible:outline-2 focus-visible:outline-offset-[-3px] focus-visible:outline-primary-500 dark:bg-dark-900"
              @click="openReview(template.id)"
            >
              <img v-if="listCoverURLs[template.id]" :src="listCoverURLs[template.id]" alt="" class="absolute inset-0 h-full w-full object-contain" />
              <div class="absolute inset-x-2 bottom-2 text-left">
                <span class="inline-flex rounded-md bg-black/45 px-1.5 py-0.5 text-[10px] font-medium text-white">
                  {{ template.aspect_ratio || '—' }}
                </span>
              </div>
              <div class="absolute left-2 top-2 flex h-7 w-7 items-center justify-center rounded-lg bg-black/30 text-white">
                <Icon name="play" size="sm" />
              </div>
            </button>

            <div class="flex min-w-0 flex-1 flex-col p-3">
              <div class="flex items-start justify-between gap-2">
                <div class="min-w-0">
                  <h2 class="line-clamp-2 break-words text-sm font-semibold leading-5 text-gray-950 dark:text-white" :title="template.title">{{ template.title }}</h2>
                  <p class="mt-1 text-xs text-gray-400">{{ t('admin.editingTemplates.owner', { id: template.owner_id }) }}</p>
                </div>
                <span class="shrink-0 rounded-md px-1.5 py-0.5 text-[10px] font-medium" :class="statusClass(template.status)">
                  {{ statusLabel(template.status) }}
                </span>
              </div>
              <p v-if="template.description" class="mt-2 line-clamp-1 text-xs leading-5 text-gray-500 dark:text-gray-400">
                {{ template.description }}
              </p>
              <dl class="mt-2 flex flex-wrap gap-x-3 gap-y-1 text-xs">
                <div class="flex items-baseline gap-1">
                  <dt class="text-gray-400">{{ t('admin.editingTemplates.duration') }}</dt>
                  <dd class="font-medium text-gray-700 dark:text-gray-200">{{ formatDuration(template.duration_ms) }}</dd>
                </div>
                <div class="flex items-baseline gap-1">
                  <dt class="text-gray-400">{{ t('admin.editingTemplates.size') }}</dt>
                  <dd class="font-medium text-gray-700 dark:text-gray-200">{{ formatBytes(template.artifacts.project?.byte_size) }}</dd>
                </div>
              </dl>
              <div class="mt-auto flex items-center justify-between gap-2 pt-3">
                <time class="truncate text-[11px] text-gray-400" :title="formatDate(template.submitted_at || template.updated_at)">{{ formatDate(template.submitted_at || template.updated_at) }}</time>
                <button type="button" class="btn btn-primary btn-sm shrink-0 whitespace-nowrap" @click="openReview(template.id)">
                  {{ t('admin.editingTemplates.view') }}
                </button>
              </div>
            </div>
          </div>
        </article>
      </section>

      <footer v-if="pagination.pages > 1" class="flex items-center justify-center gap-3">
        <button type="button" class="btn btn-secondary btn-sm" :disabled="pagination.page <= 1 || loading" @click="changePage(pagination.page - 1)">
          {{ t('admin.editingTemplates.previousPage') }}
        </button>
        <span class="text-xs text-gray-500">{{ pagination.page }} / {{ pagination.pages }}</span>
        <button type="button" class="btn btn-secondary btn-sm" :disabled="pagination.page >= pagination.pages || loading" @click="changePage(pagination.page + 1)">
          {{ t('admin.editingTemplates.nextPage') }}
        </button>
      </footer>
    </div>

    <BaseDialog :show="Boolean(selected)" :title="t('admin.editingTemplates.detailTitle')" width="extra-wide" @close="closeReview">
      <div v-if="selected" class="grid gap-6 lg:grid-cols-[minmax(0,1.35fr)_minmax(320px,.65fr)]">
        <div class="min-w-0 space-y-5">
          <div class="overflow-hidden rounded-2xl border border-gray-200 bg-black dark:border-dark-700">
            <video
              v-if="previewURL"
              class="aspect-video w-full bg-black object-contain"
              :src="previewURL"
              :poster="coverURL || undefined"
              controls
              preload="metadata"
            />
            <div v-else class="flex aspect-video items-center justify-center bg-gradient-to-br from-slate-950 to-slate-800 text-sm text-slate-300">
              {{ assetLoading ? t('common.loading') : t('admin.editingTemplates.previewUnavailable') }}
            </div>
          </div>

          <div>
            <div class="flex flex-wrap items-start justify-between gap-3">
              <div>
                <h2 class="text-xl font-semibold text-gray-950 dark:text-white">{{ selected.title }}</h2>
                <p class="mt-1 text-xs text-gray-400">{{ selected.id }}</p>
              </div>
              <span class="rounded-full px-3 py-1 text-xs font-semibold" :class="statusClass(selected.status)">{{ statusLabel(selected.status) }}</span>
            </div>
            <p class="mt-4 whitespace-pre-wrap text-sm leading-6 text-gray-600 dark:text-gray-300">{{ selected.description || '—' }}</p>
          </div>

          <div class="rounded-2xl bg-gray-50 p-4 dark:bg-dark-900/60">
            <h3 class="text-xs font-semibold uppercase tracking-wider text-gray-400">{{ t('admin.editingTemplates.briefLabel') }}</h3>
            <p class="mt-2 whitespace-pre-wrap text-sm leading-6 text-gray-700 dark:text-gray-300">{{ selected.editing_brief || '—' }}</p>
          </div>
        </div>

        <aside class="space-y-5">
          <div class="grid grid-cols-2 gap-3">
            <div class="rounded-xl border border-gray-200 p-3 dark:border-dark-700">
              <p class="text-xs text-gray-400">{{ t('admin.editingTemplates.aspectRatio') }}</p>
              <p class="mt-1 text-sm font-semibold text-gray-800 dark:text-gray-100">{{ selected.aspect_ratio || '—' }}</p>
            </div>
            <div class="rounded-xl border border-gray-200 p-3 dark:border-dark-700">
              <p class="text-xs text-gray-400">{{ t('admin.editingTemplates.duration') }}</p>
              <p class="mt-1 text-sm font-semibold text-gray-800 dark:text-gray-100">{{ formatDuration(selected.duration_ms) }}</p>
            </div>
          </div>

          <div>
            <h3 class="text-sm font-semibold text-gray-900 dark:text-white">{{ t('admin.editingTemplates.artifacts') }}</h3>
            <div class="mt-3 space-y-2">
              <div v-for="kind in artifactKinds" :key="kind" class="flex items-center justify-between rounded-xl border border-gray-200 px-3 py-2.5 dark:border-dark-700">
                <div class="min-w-0">
                  <p class="text-sm font-medium text-gray-700 dark:text-gray-200">{{ artifactLabel(kind) }}</p>
                  <p class="mt-0.5 truncate text-xs text-gray-400">{{ selected.artifacts[kind]?.original_filename || t('admin.editingTemplates.missing') }}</p>
                </div>
                <span class="ml-3 text-xs font-medium" :class="selected.artifacts[kind] ? 'text-emerald-600 dark:text-emerald-400' : 'text-amber-600 dark:text-amber-400'">
                  {{ selected.artifacts[kind] ? t('admin.editingTemplates.ready') : t('admin.editingTemplates.missing') }}
                </span>
              </div>
            </div>
          </div>

          <button
            type="button"
            class="btn btn-secondary w-full justify-center"
            :disabled="!selected.artifacts.project || downloading"
            @click="downloadProject"
          >
            <Icon name="download" size="sm" />
            {{ t('admin.editingTemplates.downloadProject') }}
          </button>

          <div v-if="selected.moderation_action" class="rounded-xl bg-blue-50 px-3 py-2 text-xs text-blue-700 dark:bg-blue-950/30 dark:text-blue-300">
            {{ t('admin.editingTemplates.moderation', { action: selected.moderation_action }) }}
          </div>

          <div>
            <h3 class="text-sm font-semibold text-gray-900 dark:text-white">{{ t('admin.editingTemplates.reviewHistory') }}</h3>
            <div v-if="selected.reviews?.length" class="mt-3 space-y-3 border-l border-gray-200 pl-4 dark:border-dark-700">
              <div v-for="review in selected.reviews" :key="review.id" class="relative">
                <span class="absolute -left-[21px] top-1.5 h-2 w-2 rounded-full bg-primary-500 ring-4 ring-white dark:ring-dark-800" />
                <div class="flex items-center justify-between gap-3">
                  <span class="text-xs font-medium text-gray-700 dark:text-gray-200">{{ statusLabel(review.to_status) }}</span>
                  <time class="text-[11px] text-gray-400">{{ formatDate(review.created_at) }}</time>
                </div>
                <p v-if="review.reason" class="mt-1 text-xs leading-5 text-gray-500 dark:text-gray-400">{{ review.reason }}</p>
              </div>
            </div>
            <p v-else class="mt-2 text-xs text-gray-400">{{ t('admin.editingTemplates.noHistory') }}</p>
          </div>

          <div v-if="selected.status === 'pending_review' || selected.status === 'approved'">
            <label class="input-label">{{ t('admin.editingTemplates.reason') }}</label>
            <textarea
              v-model.trim="reviewReason"
              class="input min-h-24 resize-y"
              :placeholder="selected.status === 'pending_review' ? t('admin.editingTemplates.rejectReasonPlaceholder') : t('admin.editingTemplates.unlistReasonPlaceholder')"
              maxlength="1000"
            />
          </div>
        </aside>
      </div>

      <template #footer>
        <div class="flex flex-wrap justify-end gap-2">
          <button type="button" class="btn btn-secondary" @click="closeReview">{{ t('common.close') }}</button>
          <button v-if="selected?.status === 'approved'" type="button" class="btn btn-danger" :disabled="acting" @click="unlistTemplate">
            {{ t('admin.editingTemplates.unlist') }}
          </button>
          <button v-if="selected?.status === 'pending_review'" type="button" class="btn btn-danger" :disabled="acting" @click="rejectTemplate">
            {{ t('admin.editingTemplates.reject') }}
          </button>
          <button v-if="selected?.status === 'pending_review'" type="button" class="btn btn-primary" :disabled="acting" @click="approveTemplate">
            {{ t('admin.editingTemplates.approve') }}
          </button>
        </div>
      </template>
    </BaseDialog>

    <TotpStepUpDialog :controller="reviewStepUp" />
  </AppLayout>
</template>

<script setup lang="ts">
import { computed, onBeforeUnmount, onMounted, ref } from 'vue'
import { useI18n } from 'vue-i18n'
import { adminAPI, type EditingTemplate, type EditingTemplateArtifactKind, type EditingTemplateStatus } from '@/api/admin'
import { useAppStore } from '@/stores'
import { useStepUp, isStepUpBlocked, isStepUpCancelled, stepUpBlockReason } from '@/composables/useStepUp'
import AppLayout from '@/components/layout/AppLayout.vue'
import BaseDialog from '@/components/common/BaseDialog.vue'
import TotpStepUpDialog from '@/components/auth/TotpStepUpDialog.vue'
import Icon from '@/components/icons/Icon.vue'

const { t } = useI18n()
const appStore = useAppStore()
const reviewStepUp = useStepUp()
const templates = ref<EditingTemplate[]>([])
const selected = ref<EditingTemplate | null>(null)
const loading = ref(false)
const assetLoading = ref(false)
const acting = ref(false)
const downloading = ref(false)
const status = ref<EditingTemplateStatus | ''>('pending_review')
const search = ref('')
const reviewReason = ref('')
const previewURL = ref('')
const coverURL = ref('')
const listCoverURLs = ref<Record<string, string>>({})
let listRevision = 0
const pagination = ref({ page: 1, page_size: 12, pages: 1, total: 0 })
const artifactKinds: EditingTemplateArtifactKind[] = ['project', 'cover', 'preview']

const statusOptions = computed(() => [
  { value: '' as const, label: t('admin.editingTemplates.allStatuses') },
  ...(['pending_review', 'approved', 'rejected', 'unlisted', 'draft'] as EditingTemplateStatus[]).map(value => ({
    value,
    label: statusLabel(value)
  }))
])

function errorMessage(error: unknown, fallback: string): string {
  if (typeof error === 'object' && error !== null && 'message' in error) {
    return String((error as { message?: unknown }).message || fallback)
  }
  return fallback
}

async function loadTemplates(): Promise<void> {
  const revision = ++listRevision
  loading.value = true
  try {
    const result = await adminAPI.editingTemplates.list({
      status: status.value,
      search: search.value,
      page: pagination.value.page,
      page_size: pagination.value.page_size
    })
    if (revision !== listRevision) return
    templates.value = result.items
    pagination.value = { page: result.page, page_size: result.page_size, pages: result.pages, total: result.total }
    revokeListCovers()
    void loadListCovers(result.items, revision)
  } catch (error) {
    if (revision === listRevision) appStore.showError(errorMessage(error, t('admin.editingTemplates.loadFailed')))
  } finally {
    if (revision === listRevision) loading.value = false
  }
}

async function loadListCovers(items: EditingTemplate[], revision: number): Promise<void> {
  await Promise.all(items.filter(item => item.artifacts.cover).map(async item => {
    try {
      const blob = await adminAPI.editingTemplates.artifact(item.id, 'cover')
      if (revision === listRevision) listCoverURLs.value[item.id] = URL.createObjectURL(blob)
    } catch {
      // A missing thumbnail leaves the card usable; review can still inspect
      // each artifact and report its own loading error.
    }
  }))
}

function revokeListCovers(): void {
  Object.values(listCoverURLs.value).forEach(value => URL.revokeObjectURL(value))
  listCoverURLs.value = {}
}

function applyFilters(): void {
  pagination.value.page = 1
  void loadTemplates()
}

function selectStatus(value: EditingTemplateStatus | ''): void {
  status.value = value
  applyFilters()
}

function changePage(page: number): void {
  pagination.value.page = page
  void loadTemplates()
}

async function openReview(id: string): Promise<void> {
  revokeAssetURLs()
  reviewReason.value = ''
  try {
    selected.value = await adminAPI.editingTemplates.get(id)
    await loadReviewAssets(selected.value)
  } catch (error) {
    appStore.showError(errorMessage(error, t('admin.editingTemplates.loadFailed')))
  }
}

async function loadReviewAssets(template: EditingTemplate): Promise<void> {
  assetLoading.value = true
  try {
    const requests: Promise<void>[] = []
    if (template.artifacts.preview) {
      requests.push(adminAPI.editingTemplates.artifact(template.id, 'preview').then(blob => { previewURL.value = URL.createObjectURL(blob) }))
    }
    if (template.artifacts.cover) {
      requests.push(adminAPI.editingTemplates.artifact(template.id, 'cover').then(blob => { coverURL.value = URL.createObjectURL(blob) }))
    }
    await Promise.all(requests)
  } catch (error) {
    appStore.showError(errorMessage(error, t('admin.editingTemplates.previewFailed')))
  } finally {
    assetLoading.value = false
  }
}

function closeReview(): void {
  selected.value = null
  reviewReason.value = ''
  revokeAssetURLs()
}

function revokeAssetURLs(): void {
  for (const value of [previewURL.value, coverURL.value]) {
    if (value) URL.revokeObjectURL(value)
  }
  previewURL.value = ''
  coverURL.value = ''
}

async function downloadProject(): Promise<void> {
  if (!selected.value?.artifacts.project) return
  downloading.value = true
  try {
    const blob = await adminAPI.editingTemplates.artifact(selected.value.id, 'project')
    const url = URL.createObjectURL(blob)
    const anchor = document.createElement('a')
    anchor.href = url
    anchor.download = selected.value.artifacts.project.original_filename || `${selected.value.id}.ccproj`
    anchor.click()
    URL.revokeObjectURL(url)
  } catch (error) {
    appStore.showError(errorMessage(error, t('admin.editingTemplates.downloadFailed')))
  } finally {
    downloading.value = false
  }
}

async function approveTemplate(): Promise<void> {
  if (!selected.value) return
  await runReviewAction(
    () => adminAPI.editingTemplates.approve(selected.value!.id),
    t('admin.editingTemplates.approveSuccess')
  )
}

async function rejectTemplate(): Promise<void> {
  if (!selected.value) return
  if (!reviewReason.value.trim()) {
    appStore.showError(t('admin.editingTemplates.reasonRequired'))
    return
  }
  await runReviewAction(
    () => adminAPI.editingTemplates.reject(selected.value!.id, reviewReason.value.trim()),
    t('admin.editingTemplates.rejectSuccess')
  )
}

async function unlistTemplate(): Promise<void> {
  if (!selected.value) return
  await runReviewAction(
    () => adminAPI.editingTemplates.unlist(selected.value!.id, reviewReason.value.trim()),
    t('admin.editingTemplates.unlistSuccess')
  )
}

async function runReviewAction(action: () => Promise<EditingTemplate>, successMessage: string): Promise<void> {
  acting.value = true
  try {
    selected.value = await reviewStepUp.run(action)
    appStore.showSuccess(successMessage)
    reviewReason.value = ''
    await loadTemplates()
  } catch (error) {
    if (isStepUpCancelled(error)) return
    if (isStepUpBlocked(error)) {
      appStore.showError(stepUpBlockReason(error) === 'STEP_UP_ADMIN_API_KEY_FORBIDDEN' ? t('stepUp.adminApiKeyForbidden') : t('stepUp.notEnabled'))
      return
    }
    appStore.showError(errorMessage(error, t('admin.editingTemplates.actionFailed')))
  } finally {
    acting.value = false
  }
}

function statusLabel(value: EditingTemplateStatus): string {
  return t(`admin.editingTemplates.status.${value}`)
}

function statusClass(value: EditingTemplateStatus): string {
  const classes: Record<EditingTemplateStatus, string> = {
    draft: 'bg-gray-100 text-gray-600 dark:bg-dark-700 dark:text-gray-300',
    pending_review: 'bg-amber-100 text-amber-700 dark:bg-amber-900/30 dark:text-amber-300',
    approved: 'bg-emerald-100 text-emerald-700 dark:bg-emerald-900/30 dark:text-emerald-300',
    rejected: 'bg-rose-100 text-rose-700 dark:bg-rose-900/30 dark:text-rose-300',
    unlisted: 'bg-slate-200 text-slate-700 dark:bg-slate-700 dark:text-slate-200'
  }
  return classes[value]
}

function artifactLabel(kind: EditingTemplateArtifactKind): string {
  return t(`admin.editingTemplates.${kind === 'project' ? 'projectPackage' : kind === 'cover' ? 'cover' : 'previewVideo'}`)
}

function formatDate(value?: string): string {
  if (!value) return '—'
  return new Intl.DateTimeFormat(undefined, { dateStyle: 'medium', timeStyle: 'short' }).format(new Date(value))
}

function formatDuration(milliseconds: number): string {
  if (!milliseconds) return '—'
  const seconds = Math.round(milliseconds / 1000)
  const minutes = Math.floor(seconds / 60)
  return `${minutes}:${String(seconds % 60).padStart(2, '0')}`
}

function formatBytes(bytes?: number): string {
  if (!bytes) return '—'
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`
  return `${(bytes / (1024 * 1024)).toFixed(1)} MB`
}

onMounted(() => { void loadTemplates() })
onBeforeUnmount(() => {
  ++listRevision
  revokeListCovers()
  revokeAssetURLs()
})
</script>

<style scoped>
.template-review-grid {
  grid-template-columns: repeat(auto-fill, minmax(min(100%, 210px), 1fr));
}
</style>
