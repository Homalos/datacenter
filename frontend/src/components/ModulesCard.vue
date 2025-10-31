<template>
  <n-card title="模块状态" hoverable>
    <n-scrollbar style="max-height: 400px">
      <n-empty
        v-if="Object.keys(modules).length === 0"
        description="暂无模块信息"
        style="padding: 40px 0"
      />
      <n-list v-else bordered>
        <n-list-item
          v-for="(info, name) in modules"
          :key="name"
          class="module-item"
        >
          <div class="module-content">
            <n-text strong class="module-name">{{ name }}</n-text>
            <n-tag
              :type="getModuleType(info.status)"
              size="small"
              round
              strong
              class="module-status"
            >
              {{ info.status }}
            </n-tag>
          </div>
        </n-list-item>
      </n-list>
    </n-scrollbar>
  </n-card>
</template>

<script setup lang="ts">
import { computed } from 'vue'
import { storeToRefs } from 'pinia'
import { useDatacenterStore } from '@/stores/datacenter'
import type { ModuleStatus } from '@/types'

const datacenterStore = useDatacenterStore()
const { state } = storeToRefs(datacenterStore)

const modules = computed(() => state.value.modules)

function getModuleType(status: ModuleStatus): 'default' | 'success' | 'warning' | 'error' | 'info' {
  const map: Record<ModuleStatus, 'default' | 'success' | 'warning' | 'error' | 'info'> = {
    'pending': 'default',
    'starting': 'warning',
    'running': 'success',
    'error': 'error',
    'registered': 'info'
  }
  return map[status] || 'default'
}
</script>

<style scoped>
.module-content {
  display: flex;
  justify-content: space-between;
  align-items: center;
  width: 100%;
  gap: 16px;
}

.module-name {
  flex: 1;
  min-width: 0;
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
  font-size: 14px;
}

.module-status {
  flex-shrink: 0;
}

/* 响应式：小屏幕时允许名称换行 */
@media (max-width: 480px) {
  .module-name {
    white-space: normal;
    word-break: break-word;
  }
}
</style>

